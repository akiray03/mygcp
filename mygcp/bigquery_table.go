package mygcp

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

type BigqueryTableLsOptions struct {
	Quiet  bool
	Fields []string
}

type BigqueryTableShowOptions struct {
	DatasetID string
	TableID   string
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

func (client *Client) BigqueryTableShow(options *BigqueryTableShowOptions) error {
	table, err := client.FetchTable(options.DatasetID, options.TableID)
	if err != nil {
		return errors.Wrap(err, "FetchTable failed:")
	}

	fmt.Fprintf(client.stdout, "%#v\n\n", table)

	for _, schema := range table.Schema {
		fmt.Fprintf(client.stdout, "%#v\n", schema)
	}

	fmt.Fprintln(client.stdout, formatTableSchema(table.Schema))

	return nil
}

func formatTableSchema(schema bigquery.Schema) string {
	maxLength := maxLengthOfSchemaName(&schema)

	var output []string
	for _, fieldSchema := range schema {
		output = append(output, formatTableFieldSchema(fieldSchema, maxLength))
	}

	return strings.Join(output[:], "\n")
}

func formatTableFieldSchema(fieldSchema *bigquery.FieldSchema, maxLength int) string {
	return fmt.Sprintf("%s\t%s", fieldSchema.Type, fieldSchema.Name)
}

func maxLengthOfSchemaName(schema *bigquery.Schema) int {
	maxLength := 0
	for _, fieldSchema := range *schema {
		if len(fieldSchema.Name) > maxLength {
			maxLength = len(fieldSchema.Name)
		}
	}
	return maxLength
}
