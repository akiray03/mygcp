package mygcp

import (
	"context"
	"io"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type Client struct {
	stdin     io.Reader
	stdout    io.Writer
	stderr    io.Writer
	profile   string
	region    string
	timezone  string
	humanize  bool
	projectID string
	ctx       context.Context
	bigquery  *bigquery.Client
}

func NewClient(stdin io.Reader, stdout io.Writer, stderr io.Writer, profile string, region string, timezone string, humanize bool, projectID string) (*Client, error) {
	ctx := context.Background()

	client := &Client{
		stdin:     stdin,
		stdout:    stdout,
		stderr:    stderr,
		profile:   profile,
		region:    region,
		timezone:  timezone,
		humanize:  humanize,
		projectID: projectID,
		ctx:       ctx,
		bigquery:  newBigqueryClient(ctx, projectID),
	}

	return client, nil
}

func newBigqueryClient(ctx context.Context, projectID string) *bigquery.Client {
	bq, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithServiceAccountFile("/Users/akira-yumiyama/work/credentials/yumiyama-6341586b50da.json"),
	)

	if err != nil {
		panic(err)
	}

	return bq
}
