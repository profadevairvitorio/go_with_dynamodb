package scenarios

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/dynamodb/actions"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/testtools"
)

func TestRunMovieScenario_Integration(t *testing.T) {
	mockQuestioner := &testtools.MockQuestioner{
		Answers: []string{
			"Test movie",
			"2001",
			"3.5",
			"Test plot.",
			"5.6",
			"New test plot.",
			"3",
			"1985",
			"2001",
			"2010",
			"y",
			"y",
			"y",
		},
	}

	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	log.SetFlags(0)
	var buf bytes.Buffer
	log.SetOutput(&buf)

	RunMovieScenario(
		sdkConfig,
		mockQuestioner,
		"doc-example-test-movie-table-integ",
		actions.MovieSampler{URL: "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip"},
	)

	log.SetOutput(os.Stderr)
	if !strings.Contains(buf.String(), "Thanks for watching") {
		t.Errorf("didn't run to successful completion. Here's the log:\n%v", buf.String())
	}
}