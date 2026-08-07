// cmd/scheduler is the control-plane binary: gRPC server for workers, REST
// server for humans/CI, the dead-worker reaper, and the scheduling loop
// itself, all gated behind Postgres-advisory-lock leader
// election (docs/09-design-rationale.md 9.6) so that running more than one
// replica is safe: only the elected leader reaps or schedules, and every
// replica still serves REST/gRPC traffic.
package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/launchverse/fleetforge/internal/api"
	"github.com/launchverse/fleetforge/internal/auth"
	"github.com/launchverse/fleetforge/internal/config"
	"github.com/launchverse/fleetforge/internal/grpcserver"
	"github.com/launchverse/fleetforge/internal/metrics"
	"github.com/launchverse/fleetforge/internal/scheduler"
	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "scheduler").Logger()

	cfg, err := config.LoadSchedulerConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("invalid config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		//nolint:gocritic // process is exiting immediately; the pending defer stop() above has nothing left to clean up
		log.Fatal().Err(err).Msg("failed to connect to postgres")
	}
	defer pool.Close()

	workerStore := postgres.NewWorkerStore(pool)
	jobStore := postgres.NewJobStore(pool)

	redisClient, err := ffredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.RedisAddr).Msg("failed to connect to redis")
	}
	defer func() { _ = redisClient.Close() }()
	workerCache := ffredis.NewWorkerCache(redisClient)

	jobQueue := ffredis.NewStreamQueue(redisClient)
	if groupErr := jobQueue.EnsureConsumerGroups(ctx); groupErr != nil {
		log.Fatal().Err(groupErr).Msg("failed to set up redis consumer groups")
	}

	// --- gRPC control plane (workers) ---
	//
	// mTLS (docs/09-design-rationale.md 9.4): only enabled when all three
	// cert/key/CA paths are configured, so every existing local-dev and CI
	// setup (none of which set these) is completely unaffected. See
	// scripts/gen-certs.sh to generate a cert pair and turn this on.
	var grpcOpts []grpc.ServerOption
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" && cfg.TLSCAFile != "" {
		tlsConfig, tlsErr := auth.ServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if tlsErr != nil {
			//nolint:gocritic // process is exiting immediately; the pending defer stop() above has nothing left to clean up
			log.Fatal().Err(tlsErr).Msg("failed to load mTLS server config")
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
		log.Info().Msg("grpc control plane: mTLS enabled, client certificates required")
	} else {
		log.Warn().Msg("grpc control plane: mTLS NOT configured, running with insecure credentials (set FLEETFORGE_TLS_CERT_FILE/KEY_FILE/CA_FILE)")
	}

	grpcSrv := grpc.NewServer(grpcOpts...)
	fleetforgev1.RegisterFleetSchedulerServer(grpcSrv, grpcserver.NewServer(
		workerStore,
		jobStore,
		workerCache,
		log,
		int32(cfg.HeartbeatIntervalSeconds),
		int32(cfg.HeartbeatTimeoutSeconds),
	))

	// --- Leader election (docs/09-design-rationale.md 9.6) ---
	leaderElector := scheduler.NewLeaderElector(pool, log)
	go leaderElector.Run(ctx, 2*time.Second)

	// --- Dead-worker reaper (doc 5.3), leader-gated ---
	reaper := scheduler.NewReaper(
		workerStore,
		workerCache,
		log,
		5*time.Second, // sweep interval; see docs/05-sequence-diagrams.md 5.3
		cfg.HeartbeatTimeoutSeconds,
	)
	go reaper.Run(ctx, leaderElector.IsLeader)

	// --- Retry backoff poller (docs/09-design-rationale.md 9.3) ---
	// Leader-gated, same reasoning as the reaper above.
	retryPoller := scheduler.NewRetryPoller(jobStore, jobQueue, log, 2*time.Second)
	go retryPoller.Run(ctx, leaderElector.IsLeader)

	// --- Metrics ---
	// Registered once, here, against the DEFAULT registry: promhttp.Handler()
	// in internal/api/router.go reads from that same default registry/gatherer,
	// so no Registry object needs to be threaded through main() and the router.
	metrics.MustRegister(prometheus.DefaultRegisterer)
	metricsCollector := metrics.NewCollector(jobStore, workerStore, log, 10*time.Second)
	go metricsCollector.Run(ctx)

	// --- Autoscaler (docs/09-design-rationale.md 9.2), leader-gated ---
	autoscaler := scheduler.NewAutoscaler(
		workerStore,
		jobStore,
		workerCache,
		log,
		scheduler.DefaultAutoscalerConfig(),
		15*time.Second, // decision interval; independent of the cooldown windows inside AutoscalerConfig, which bound how often it actually ACTS
	)
	go autoscaler.Run(ctx, leaderElector.IsLeader)

	// --- Scheduling loop (doc 5.4), leader-gated ---
	//
	// Deliberately a FIXED consumer name, not a random one per process
	// start: Redis tracks pending stream entries per consumer NAME. Only
	// the elected leader ever actively reads from the consumer group at a
	// time, so there's never a real collision to worry about. A random
	// name per restart would mean a restarted (or newly-elected) scheduler
	// could never see its own previous run's still-pending, not-yet-acked
	// entries, which would then sit stuck forever under a consumer
	// identity nothing will ever check again. A fixed name is what makes
	// doc 4.1's "pending entries survive a restart" property actually
	// true.
	consumerName := "scheduler-leader"
	schedulingLoop := scheduler.NewLoop(
		workerStore,
		jobStore,
		workerCache,
		redisClient,
		log,
		consumerName,
		leaderElector.IsLeader,
	)
	go schedulingLoop.Run(ctx)

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.GRPCAddr).Msg("failed to bind grpc listener")
	}

	go func() {
		log.Info().Str("addr", cfg.GRPCAddr).Msg("grpc control server listening")
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error().Err(err).Msg("grpc server stopped")
		}
	}()

	// --- REST API (humans/CI) ---
	isReady := func() (pgOK, redisOK, isLeader bool) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		pgOK = pool.Ping(pingCtx) == nil
		redisOK = redisClient.Ping(pingCtx).Err() == nil
		return pgOK, redisOK, leaderElector.IsLeader()
	}

	if cfg.JWTSecret == "" {
		log.Warn().Msg("rest api: FLEETFORGE_JWT_SECRET not set, write endpoints (POST /jobs, drain/resume) are unauthenticated")
	}

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewRouter(workerStore, jobStore, jobQueue, workerCache, log, isReady, []byte(cfg.JWTSecret)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().Str("addr", cfg.HTTPAddr).Msg("rest api listening")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http server stopped")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("http graceful shutdown failed")
	}
	grpcSrv.GracefulStop()
}
