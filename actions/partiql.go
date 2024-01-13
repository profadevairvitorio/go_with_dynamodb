package actions

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PartiQLRunner struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}


func (runner PartiQLRunner) AddMovie(movie Movie) error {
	params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year, movie.Info})
	if err != nil {
		panic(err)
	}
	_, err = runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(
			fmt.Sprintf("INSERT INTO \"%v\" VALUE {'title': ?, 'year': ?, 'info': ?}",
				runner.TableName)),
		Parameters: params,
	})
	if err != nil {
		log.Printf("Couldn't insert an item with PartiQL. Here's why: %v\n", err)
	}
	return err
}

func (runner PartiQLRunner) GetMovie(title string, year int) (Movie, error) {
	var movie Movie
	params, err := attributevalue.MarshalList([]interface{}{title, year})
	if err != nil {
		panic(err)
	}
	response, err := runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(
			fmt.Sprintf("SELECT * FROM \"%v\" WHERE title=? AND year=?",
				runner.TableName)),
		Parameters: params,
	})
	if err != nil {
		log.Printf("Couldn't get info about %v. Here's why: %v\n", title, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Items[0], &movie)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return movie, err
}


func (runner PartiQLRunner) GetAllMovies() ([]map[string]interface{}, error) {
	var output []map[string]interface{}
	response, err := runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(
			fmt.Sprintf("SELECT title, info.rating FROM \"%v\"", runner.TableName)),
	})
	if err != nil {
		log.Printf("Couldn't get movies. Here's why: %v\n", err)
	} else {
		err = attributevalue.UnmarshalListOfMaps(response.Items, &output)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return output, err
}


func (runner PartiQLRunner) UpdateMovie(movie Movie, rating float64) error {
	params, err := attributevalue.MarshalList([]interface{}{rating, movie.Title, movie.Year})
	if err != nil {
		panic(err)
	}
	_, err = runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(
			fmt.Sprintf("UPDATE \"%v\" SET info.rating=? WHERE title=? AND year=?",
				runner.TableName)),
		Parameters: params,
	})
	if err != nil {
		log.Printf("Couldn't update movie %v. Here's why: %v\n", movie.Title, err)
	}
	return err
}


func (runner PartiQLRunner) DeleteMovie(movie Movie) error {
	params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year})
	if err != nil {
		panic(err)
	}
	_, err = runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(
			fmt.Sprintf("DELETE FROM \"%v\" WHERE title=? AND year=?",
				runner.TableName)),
		Parameters: params,
	})
	if err != nil {
		log.Printf("Couldn't delete %v from the table. Here's why: %v\n", movie.Title, err)
	}
	return err
}


func (runner PartiQLRunner) AddMovieBatch(movies []Movie) error {
	statementRequests := make([]types.BatchStatementRequest, len(movies))
	for index, movie := range movies {
		params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year, movie.Info})
		if err != nil {
			panic(err)
		}
		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(fmt.Sprintf(
				"INSERT INTO \"%v\" VALUE {'title': ?, 'year': ?, 'info': ?}", runner.TableName)),
			Parameters: params,
		}
	}

	_, err := runner.DynamoDbClient.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	if err != nil {
		log.Printf("Couldn't insert a batch of items with PartiQL. Here's why: %v\n", err)
	}
	return err
}

func (runner PartiQLRunner) GetMovieBatch(movies []Movie) ([]Movie, error) {
	statementRequests := make([]types.BatchStatementRequest, len(movies))
	for index, movie := range movies {
		params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year})
		if err != nil {
			panic(err)
		}
		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(
				fmt.Sprintf("SELECT * FROM \"%v\" WHERE title=? AND year=?", runner.TableName)),
			Parameters: params,
		}
	}

	output, err := runner.DynamoDbClient.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	var outMovies []Movie
	if err != nil {
		log.Printf("Couldn't get a batch of items with PartiQL. Here's why: %v\n", err)
	} else {
		for _, response := range output.Responses {
			var movie Movie
			err = attributevalue.UnmarshalMap(response.Item, &movie)
			if err != nil {
				log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
			} else {
				outMovies = append(outMovies, movie)
			}
		}
	}
	return outMovies, err
}

func (runner PartiQLRunner) UpdateMovieBatch(movies []Movie, ratings []float64) error {
	statementRequests := make([]types.BatchStatementRequest, len(movies))
	for index, movie := range movies {
		params, err := attributevalue.MarshalList([]interface{}{ratings[index], movie.Title, movie.Year})
		if err != nil {
			panic(err)
		}
		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(
				fmt.Sprintf("UPDATE \"%v\" SET info.rating=? WHERE title=? AND year=?", runner.TableName)),
			Parameters: params,
		}
	}

	_, err := runner.DynamoDbClient.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	if err != nil {
		log.Printf("Couldn't update the batch of movies. Here's why: %v\n", err)
	}
	return err
}

func (runner PartiQLRunner) DeleteMovieBatch(movies []Movie) error {
	statementRequests := make([]types.BatchStatementRequest, len(movies))
	for index, movie := range movies {
		params, err := attributevalue.MarshalList([]interface{}{movie.Title, movie.Year})
		if err != nil {
			panic(err)
		}
		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(
				fmt.Sprintf("DELETE FROM \"%v\" WHERE title=? AND year=?", runner.TableName)),
			Parameters: params,
		}
	}

	_, err := runner.DynamoDbClient.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	if err != nil {
		log.Printf("Couldn't delete the batch of movies. Here's why: %v\n", err)
	}
	return err
}
