package mygcp

import (
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Dataset struct {
	ProjectID              string
	DatasetID              string
	CreationTime           time.Time
	LastModifiedTime       time.Time // When the dataset or any of its tables were modified.
	DefaultTableExpiration time.Duration
	Description            string            // The user-friendly description of this table.
	Name                   string            // The user-friendly name for this table.
	Location               string            // The geo location of the dataset.
	Labels                 map[string]string // User-provided labels.
	Tables                 []*Table
}

type Table struct {
	ProjectID string
	DatasetID string
	TableID   string

	Description string // The user-friendly description of this table.
	Name        string // The user-friendly name for this table.
	Schema      bigquery.Schema
	View        string

	ID   string // An opaque ID uniquely identifying the table.
	Type bigquery.TableType

	// The time when this table expires. If not set, the table will persist
	// indefinitely. Expired tables will be deleted and their storage reclaimed.
	ExpirationTime time.Time

	CreationTime     time.Time
	LastModifiedTime time.Time

	// The size of the table in bytes.
	// This does not include data that is being buffered during a streaming insert.
	NumBytes int64

	// The number of rows of data in this table.
	// This does not include data that is being buffered during a streaming insert.
	NumRows uint64

	// The time-based partitioning settings for this table.
	TimePartitioning *bigquery.TimePartitioning
}

func (client *Client) FetchDatasetList(includeTable bool) ([]*Dataset, error) {
	var dslist []*Dataset

	it := client.bigquery.Datasets(client.ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return dslist, err
		}

		metadata, err := ds.Metadata(client.ctx)
		if err != nil {
			return dslist, err
		}

		dataset := convertToDataset(ds, metadata)

		if includeTable {
			tables, err := client.FetchTableList(dataset)
			if err != nil {
				return dslist, err
			}

			dataset.Tables = tables
		}

		dslist = append(dslist, dataset)
	}

	return dslist, nil
}

func convertToDataset(ds *bigquery.Dataset, metadata *bigquery.DatasetMetadata) *Dataset {
	dataset := &Dataset{}

	dataset.ProjectID = ds.ProjectID
	dataset.DatasetID = ds.DatasetID

	dataset.CreationTime = metadata.CreationTime
	dataset.LastModifiedTime = metadata.LastModifiedTime
	dataset.DefaultTableExpiration = metadata.DefaultTableExpiration
	dataset.Description = metadata.Description
	dataset.Name = metadata.Name
	dataset.Location = metadata.Location
	dataset.Labels = metadata.Labels

	return dataset
}

func (client *Client) FetchTableList(dataset *Dataset) ([]*Table, error) {
	var tablelist []*Table

	it := client.bigquery.Dataset(dataset.DatasetID).Tables(client.ctx)
	for {
		table := &Table{}

		tbl, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return tablelist, err
		}

		table.ProjectID = tbl.ProjectID
		table.DatasetID = tbl.DatasetID
		table.TableID = tbl.TableID

		metadata, err := tbl.Metadata(client.ctx)
		if err != nil {
			return tablelist, err
		}
		table.Description = metadata.Description
		table.Name = metadata.Name
		table.Schema = metadata.Schema
		table.View = metadata.View
		table.ID = metadata.ID
		table.Type = metadata.Type
		table.ExpirationTime = metadata.ExpirationTime
		table.CreationTime = metadata.CreationTime
		table.LastModifiedTime = metadata.LastModifiedTime
		table.NumBytes = metadata.NumBytes
		table.NumRows = metadata.NumRows
		table.TimePartitioning = metadata.TimePartitioning

		tablelist = append(tablelist, table)
	}

	return tablelist, nil
}
