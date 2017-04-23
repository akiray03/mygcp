package cmd

import (
	"github.com/akiray03/mygcp/mygcp"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func BigqueryTableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table",
		Short: "Manage Table resources",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	cmd.AddCommand(
		newBQTableLsCmd(),
		newBQTableShowCmd(),
	)

	return cmd
}

func newBQTableLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List Tables",
		RunE:  runBQTableLsCmd,
	}

	return cmd
}

func runBQTableLsCmd(command *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	options := &mygcp.BigqueryTableLsOptions{}

	return client.BigqueryTableLs(options)
}

func newBQTableShowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show Table details",
		RunE:  runBQTableShowCmd,
	}

	return cmd
}

func runBQTableShowCmd(command *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	if len(args) != 2 {
		return errors.New("TableID is required")
	}

	datasetID := args[0]
	tableID := args[1]

	options := &mygcp.BigqueryTableShowOptions{
		DatasetID: datasetID,
		TableID:   tableID,
	}

	return client.BigqueryTableShow(options)
}
