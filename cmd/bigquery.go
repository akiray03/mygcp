package cmd

import "github.com/spf13/cobra"

func init() {
	RootCmd.AddCommand(newBQCmd())
}

func newBQCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bq",
		Short: "Manage BigQuery resources",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	cmd.AddCommand(
		BigqueryDatasetCmd(),
		BigqueryTableCmd(),
	)

	return cmd
}
