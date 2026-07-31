// fleetforgectl is the operator CLI: worker listing (`fleetforgectl workers
// list`) and the drain/resume lifecycle (`fleetforgectl workers
// drain|resume <id>`), each a plain net/http call against the REST API, plus
// `fleetforgectl auth mint-token` for issuing the bearer tokens
// docs/09-design-rationale.md 9.4 requires for those write endpoints once
// FLEETFORGE_JWT_SECRET is set on the scheduler.
//
// docs/07-repository-structure.md calls for pkg/fleetforgeclient, a client
// generated from openapi.yaml, to back this CLI eventually -- deferred
// until the REST surface stabilizes further, since generating a client
// against a still-growing API is more scaffolding than the CLI itself.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/launchverse/fleetforge/internal/auth"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var apiAddr, token string

	root := &cobra.Command{
		Use:   "fleetforgectl",
		Short: "Operator CLI for the FleetForge build scheduler",
	}
	root.PersistentFlags().StringVar(&apiAddr, "api", "http://localhost:8080", "FleetForge REST API base URL")
	root.PersistentFlags().StringVar(&token, "token", "", "bearer token for write endpoints (defaults to FLEETFORGE_TOKEN env var; only needed if the scheduler has FLEETFORGE_JWT_SECRET set -- see 'fleetforgectl auth mint-token')")

	root.AddCommand(newWorkersCmd(&apiAddr, &token))
	root.AddCommand(newAuthCmd())
	return root
}

func newAuthCmd() *cobra.Command {
	authCmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage bearer tokens for the REST API's write endpoints",
	}

	var (
		secret    string
		subject   string
		scopesRaw string
		ttl       time.Duration
	)
	mintToken := &cobra.Command{
		Use:   "mint-token",
		Short: "Issue a bearer token (requires the same secret configured as FLEETFORGE_JWT_SECRET on the scheduler)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if secret == "" {
				secret = os.Getenv("FLEETFORGE_JWT_SECRET")
			}
			if secret == "" {
				return fmt.Errorf("no secret: pass --secret or set FLEETFORGE_JWT_SECRET")
			}
			var scopes []string
			for _, s := range strings.Split(scopesRaw, ",") {
				if s = strings.TrimSpace(s); s != "" {
					scopes = append(scopes, s)
				}
			}
			if len(scopes) == 0 {
				return fmt.Errorf("--scopes is required (comma-separated, e.g. %s,%s)", auth.ScopeJobsSubmit, auth.ScopeWorkersDrain)
			}

			token, err := auth.IssueToken([]byte(secret), subject, scopes, ttl)
			if err != nil {
				return fmt.Errorf("mint token: %w", err)
			}
			fmt.Println(token)
			return nil
		},
	}
	mintToken.Flags().StringVar(&secret, "secret", "", "signing secret (defaults to FLEETFORGE_JWT_SECRET env var)")
	mintToken.Flags().StringVar(&subject, "subject", "fleetforgectl", "identifies who/what this token was issued for (audit only, not enforced)")
	mintToken.Flags().StringVar(&scopesRaw, "scopes", "", "comma-separated scopes, e.g. "+auth.ScopeJobsSubmit+","+auth.ScopeWorkersDrain)
	mintToken.Flags().DurationVar(&ttl, "ttl", time.Hour, "token lifetime")

	authCmd.AddCommand(mintToken)
	return authCmd
}

func newWorkersCmd(apiAddr, token *string) *cobra.Command {
	workers := &cobra.Command{
		Use:   "workers",
		Short: "Inspect registered workers",
	}

	var statusFilter string
	list := &cobra.Command{
		Use:   "list",
		Short: "List registered workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listWorkers(*apiAddr, statusFilter)
		},
	}
	list.Flags().StringVar(&statusFilter, "status", "", "filter by worker status (READY, BUSY, DRAINING, OFFLINE, DEAD)")

	drain := &cobra.Command{
		Use:   "drain <worker-id>",
		Short: "Request graceful drain of a worker (finishes its current job, then refuses new work)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postWorkerAction(*apiAddr, resolveToken(*token), args[0], "drain")
		},
	}

	resume := &cobra.Command{
		Use:   "resume <worker-id>",
		Short: "Undo a prior drain request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postWorkerAction(*apiAddr, resolveToken(*token), args[0], "resume")
		},
	}

	workers.AddCommand(list, drain, resume)
	return workers
}

// resolveToken falls back to FLEETFORGE_TOKEN so scripts/CI can set it once
// as an env var rather than passing --token on every invocation.
func resolveToken(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv("FLEETFORGE_TOKEN")
}

func postWorkerAction(apiAddr, token, workerID, action string) error {
	url := apiAddr + "/v1/workers/" + workerID + "/" + action

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(nil)) //nolint:gosec // operator-supplied base URL/flag, not user input from the network
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Println(string(body))
	return nil
}

type workerRow struct {
	ID                string `json:"id"`
	Hostname          string `json:"hostname"`
	Status            string `json:"status"`
	Version           string `json:"version"`
	AvailableCapacity int32  `json:"available_capacity"`
	Epoch             int64  `json:"epoch"`
	RegisteredAt      string `json:"registered_at"`
}

type workerListResponse struct {
	Items      []workerRow `json:"items"`
	TotalCount int         `json:"total_count"`
}

func listWorkers(apiAddr, statusFilter string) error {
	url := apiAddr + "/v1/workers"
	if statusFilter != "" {
		url += "?status=" + statusFilter
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil) //nolint:gosec // operator-supplied base URL/flag, not user input from the network
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var out workerListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tHOSTNAME\tSTATUS\tCAPACITY\tEPOCH\tVERSION\tREGISTERED"); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	for _, w := range out.Items {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			w.ID, w.Hostname, w.Status, w.AvailableCapacity, w.Epoch, w.Version, w.RegisteredAt); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return tw.Flush()
}
