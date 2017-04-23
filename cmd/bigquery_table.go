package cmd

import (
	"github.com/akiray03/mygcp/mygcp"
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
