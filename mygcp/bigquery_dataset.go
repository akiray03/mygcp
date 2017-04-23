package mygcp

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type BigqueryDatasetLsOptions struct {
	Quiet  bool
	Fields []string
}

func (client *Client) BigqueryDatasetLs(options *BigqueryDatasetLsOptions) error {
	fmt.Fprintf(client.stdout, "BqDataSetLs start ...\n")

	datasets, err := client.FetchDatasetList(false)
	if err != nil {
		return errors.Wrap(err, "FetchDatasetList failed:")
	}

	for _, dataset := range datasets {
		fmt.Fprintln(client.stdout, formatBQDataset(client, options, dataset))
	}

	fmt.Fprintf(client.stdout, "BqDataSetLs done.\n")
	return nil
}

func formatBQDataset(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	formatFuncs := map[string]func(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string{
		"ProjectID":              formatBqProjectID,
		"DatasetID":              formatBqDatasetID,
		"CreationTime":           formatBqDatasetCreationTime,
		"LastModified":           formatBqDatasetLastModified,
		"DefaultTableExpiration": formatBqDatasetDefaultTableExpiration,
		"Description":            formatBqDatasetDescription,
		"Name":                   formatBqDatasetName,
		"Location":               formatBqDatasetLocation,
		"Labels":                 formatBqDatasetLabels,
	}

	outputFields := []string{
		"ProjectID",
		"DatasetID",
		"CreationTime",
		"LastModified",
		"DefaultTableExpiration",
		"Name",
		"Location",
		"Description",
	}

	output := []string{}

	for _, field := range outputFields {
		value := formatFuncs[field](client, options, dataset)
		output = append(output, value)
	}

	return strings.Join(output[:], "\t")
}

func formatBqProjectID(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return dataset.ProjectID
}

func formatBqDatasetID(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return dataset.DatasetID
}

func formatBqDatasetDescription(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	if dataset.Description == "" {
		return "-"
	}
	return dataset.Description
}

func formatBqDatasetCreationTime(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return dataset.CreationTime.Format("2006-01-02")
}

func formatBqDatasetLastModified(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return dataset.LastModifiedTime.Format("2006-01-02")
}

func formatBqDatasetDefaultTableExpiration(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	if dataset.DefaultTableExpiration.Hours() == 0.0 {
		return "Never"
	}
	return fmt.Sprintf("%d days", int64(dataset.DefaultTableExpiration.Hours()/24))
}

func formatBqDatasetName(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return dataset.Name
}

func formatBqDatasetLocation(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	if dataset.Location == "" {
		return "default:US"
	}
	return dataset.Location
}

func formatBqDatasetLabels(client *Client, options *BigqueryDatasetLsOptions, dataset *Dataset) string {
	return fmt.Sprintf("%v", dataset.Labels)
}
