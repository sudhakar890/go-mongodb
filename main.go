package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Podcast model
type Podcast struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Title  string             `bson:"title,omitempty"`
	Author string             `bson:"author,omitempty"`
	Tags   []string           `bson:"tags,omitempty"`
}

// Episode model
type Episode struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Podcast     primitive.ObjectID `bson:"podcast,omitempty"`
	Title       string             `bson:"title,omitempty"`
	Description string             `bson:"description,omitempty"`
	Duration    int32              `bson:"duration,omitempty"`
}

func main() {
	// Mongodb Client
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb+srv://user1:user123@cluster0-9tn5d.azure.mongodb.net/test?retryWrites=true&w=majority"))
	if err != nil {
		log.Panic(err)
	}

	// Context for timeout of 10 secs
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	err = client.Connect(ctx)
	if err != nil {
		log.Panic(err)
	}
	defer client.Disconnect(ctx)

	quickstartDatabase := client.Database("quickstart")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	//Insert one document
	mongoPodcast := Podcast{
		Title:  "The Polyglot Developer Podcast",
		Author: "Nic Raboy",
		Tags:   []string{"development", "programming", "coding"},
	}
	podcastResult, err := podcastsCollection.InsertOne(ctx, mongoPodcast)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("Inserted document %v into podcast collection!\n", podcastResult.InsertedID)

	oid := podcastResult.InsertedID.(primitive.ObjectID)

	//Insert many documents
	mongoEpisodes := []interface{}{
		Episode{
			Podcast:     oid,
			Title:       "GraphQL for API Development",
			Description: "Learn about GraphQL from the co-creator of GraphQL, Lee Byron.",
			Duration:    25,
		},
		Episode{
			Podcast:     oid,
			Title:       "Progressive Web Application Development",
			Description: "Learn about PWA development with Tara Manicsic.",
			Duration:    32,
		},
	}
	episodeResult, err := episodesCollection.InsertMany(ctx, mongoEpisodes)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("Inserted %v documents into episode collection!\n", len(episodeResult.InsertedIDs))

	//Retrieve all documents from coll

	//This is more memory intensive if there are large no of docs
	cursor, err := episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Panic(err)
	}
	var episodes []bson.M
	if err := cursor.All(ctx, &episodes); err != nil {
		log.Panic(err)
	}
	fmt.Println(episodes)

	//This one is effective for large no of docs
	cursor, err = episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var episode bson.M
		if err := cursor.Decode(&episode); err != nil {
			log.Panic(err)
		}
		fmt.Println(episode)
	}

	//Retrieve one document from coll
	var podcast bson.M
	if err = podcastsCollection.FindOne(ctx, bson.M{}).Decode(&podcast); err != nil {
		log.Panic(err)
	}
	fmt.Println(podcast)

	//Filter the documents from coll
	fliterCursor, err := episodesCollection.Find(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Panic(err)
	}
	var episodesFiltered []bson.M
	if err = fliterCursor.All(ctx, &episodesFiltered); err != nil {
		log.Panic(err)
	}
	fmt.Println(episodesFiltered)

	//Sort the documents in decending order of duration
	opts := options.Find()
	opts.SetSort(bson.D{{"duration", -1}})
	sortCursor, err := episodesCollection.Find(ctx, bson.D{
		{"duration", bson.D{{"$gt", 22}}},
	}, opts)
	var episodesSorted []bson.M //order doesnt matter
	if err = sortCursor.All(ctx, &episodesSorted); err != nil {
		log.Panic(err)
	}
	fmt.Println(episodesSorted)

	//Update one docuement
	id, _ := primitive.ObjectIDFromHex("5e70b9d53343ba1d8a6de818")
	result, err := podcastsCollection.UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.D{
			{"$set", bson.D{{"author", "Nicolas Roby"}}},
		},
	)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("Updated %v documents\n", result.ModifiedCount)

	//Replace whole doc
	result, err = podcastsCollection.ReplaceOne(
		ctx,
		bson.M{"title": "The Polyglot Developer Podcast"},
		bson.M{
			"title":  "The Sudhakar Nandigam show",
			"author": "Sudhakar N",
		},
	)
	fmt.Printf("Updated %v documents\n", result.ModifiedCount)

	//Delete a document
	delResult, err := episodesCollection.DeleteOne(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("deleted %v documents\n", delResult.DeletedCount)

}
