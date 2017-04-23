package cmd

import (
	"github.com/akiray03/mygcp/mygcp"
	"github.com/spf13/cobra"
)

func BigqueryDatasetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Manage Dataset resources",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	cmd.AddCommand(
		newBQDatasetLsCmd(),
	)

	return cmd
}

func newBQDatasetLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List DataSets",
		RunE:  runBQDatasetLsCmd,
	}

	return cmd
}

func runBQDatasetLsCmd(command *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	options := &mygcp.BigqueryDatasetLsOptions{}

	return client.BigqueryDatasetLs(options)
}
