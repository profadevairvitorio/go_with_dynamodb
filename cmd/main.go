package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/demotools"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/dynamodb/actions"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/dynamodb/scenarios"
)

func main() {
	scenarioMap := map[string]func(sdkConfig aws.Config){
		"movieTable":    runMovieScenario,
		"partiQLSingle": runPartiQLSingleScenario,
		"partiQLBatch":  runPartiQLBatchScenario,
	}
	choices := make([]string, len(scenarioMap))
	choiceIndex := 0
	for choice := range scenarioMap {
		choices[choiceIndex] = choice
		choiceIndex++
	}
	scenario := flag.String(
		"scenario", "",
		fmt.Sprintf("The scenario to run. Must be one of %v.", choices))
	flag.Parse()

	if runScenario, ok := scenarioMap[*scenario]; !ok {
		fmt.Printf("'%v' is not a valid scenario.\n", *scenario)
		flag.Usage()
	} else {
		sdkConfig, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}

		log.SetFlags(0)
		runScenario(sdkConfig)
	}
}

func runMovieScenario(sdkConfig aws.Config) {
	scenarios.RunMovieScenario(
		sdkConfig,
		demotools.NewQuestioner(),
		"doc-example-movie-table",
		actions.MovieSampler{URL: "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip"},
	)
}

func runPartiQLSingleScenario(sdkConfig aws.Config) {
	scenarios.RunPartiQLSingleScenario(sdkConfig, "doc-example-partiql-single-table")
}

func runPartiQLBatchScenario(sdkConfig aws.Config) {
	scenarios.RunPartiQLBatchScenario(sdkConfig, "doc-example-partiql-batch-table")
}
