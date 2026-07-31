// fleetforgectl is the operator CLI: worker listing (`fleetforgectl workers
// list`) and the drain/resume lifecycle (`fleetforgectl workers
// drain|resume <id>`), each a plain net/http call against the REST API.
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
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var apiAddr string

	root := &cobra.Command{
		Use:   "fleetforgectl",
		Short: "Operator CLI for the FleetForge build scheduler",
	}
	root.PersistentFlags().StringVar(&apiAddr, "api", "http://localhost:8080", "FleetForge REST API base URL")

	root.AddCommand(newWorkersCmd(&apiAddr))
	return root
}

func newWorkersCmd(apiAddr *string) *cobra.Command {
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
			return postWorkerAction(*apiAddr, args[0], "drain")
		},
	}

	resume := &cobra.Command{
		Use:   "resume <worker-id>",
		Short: "Undo a prior drain request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postWorkerAction(*apiAddr, args[0], "resume")
		},
	}

	workers.AddCommand(list, drain, resume)
	return workers
}

func postWorkerAction(apiAddr, workerID, action string) error {
	url := apiAddr + "/v1/workers/" + workerID + "/" + action

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(nil)) //nolint:gosec // operator-supplied base URL/flag, not user input from the network
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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
