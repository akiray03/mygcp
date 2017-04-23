package mygcp

import (
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

type BigqueryTableLsOptions struct {
	Quiet  bool
	Fields []string
}

func (client *Client) BigqueryTableLs(options *BigqueryTableLsOptions) error {
	fmt.Fprintf(client.stdout, "BqDataSetLs start ...\n")

	datasets, err := client.FetchDatasetList(true)
	if err != nil {
		return errors.Wrap(err, "FetchDatasetList failed:")
	}

	for _, dataset := range datasets {
		for _, table := range dataset.Tables {
			fmt.Fprintln(client.stdout, formatBQTable(client, options, table))
		}
	}

	fmt.Fprintf(client.stdout, "BqDataSetLs done.\n")
	return nil
}

func formatBQTable(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	formatFuncs := map[string]func(client *Client, options *BigqueryTableLsOptions, table *Table) string{
		"ProjectID":         formatBqTableProjectID,
		"DatasetID":         formatBqTableDatasetID,
		"TableID":           formatBqTableID,
		"Name":              formatBqTableName,
		"Description":       formatBqTableDescription,
		"TableSize":         formatBqTableSize,
		"TableRows":         formatBqTableRows,
		"TableCreationTime": formatBqTableCreationTime,
		"TableLastModified": formatBqTableLastModified,
	}

	outputFields := []string{
		"DatasetID",
		"TableID",
		"Name",
		"Description",
		"TableSize",
		"TableRows",
		"TableCreationTime",
		"TableLastModified",
	}

	output := []string{}

	for _, field := range outputFields {
		value := formatFuncs[field](client, options, table)
		output = append(output, value)
	}

	return strings.Join(output[:], "\t")
}

func formatBqTableProjectID(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return table.ProjectID
}

func formatBqTableDatasetID(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return table.DatasetID
}

func formatBqTableID(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return table.TableID
}

func formatBqTableName(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	if table.Name == "" {
		return "-"
	}
	return table.Name
}

func formatBqTableDescription(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	if table.Description == "" {
		return "-"
	}
	return table.Description
}

func formatBqTableSize(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return humanize.Bytes(uint64(table.NumBytes))
}

func formatBqTableRows(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return humanize.SI(float64(table.NumRows), "rows")
}

func formatBqTableCreationTime(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return table.CreationTime.Format("2006-01-02")
}

func formatBqTableLastModified(client *Client, options *BigqueryTableLsOptions, table *Table) string {
	return table.LastModifiedTime.Format("2006-01-02")
}
