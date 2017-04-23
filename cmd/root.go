package cmd

import (
	"github.com/akiray03/mygcp/mygcp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var cfgFile string

var RootCmd = &cobra.Command{
	Use:           "mygcp",
	Short:         "A human friendly GCP CLI written in Go.",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().BoolP("debug", "", false, "Enable debug mode")
	RootCmd.PersistentFlags().StringP("projectID", "", "spry-firefly-576", "GCP Project ID")

	viper.BindPFlag("debug", RootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("projectID", RootCmd.PersistentFlags().Lookup("projectID"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	}

	viper.SetConfigName(".mygcp")
	viper.AddConfigPath("$HOME")
	viper.AutomaticEnv()

	viper.ReadInConfig()
}

func newClient() (*mygcp.Client, error) {
	return mygcp.NewClient(
		os.Stdin,
		os.Stdout,
		os.Stderr,
		viper.GetString("profile"),
		viper.GetString("region"),
		viper.GetString("timezone"),
		viper.GetBool("humanize"),
		viper.GetString("projectID"),
	)
}
