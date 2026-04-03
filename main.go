package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	sourceURI = "mongodb://localhost:27010/?replicaSet=rs0&directConnection=true"
	targetURI = "mongodb://localhost:27021/"
	dbName    = "pdv"
)

func main() {
	ctx := context.Background()

	source, _ := mongo.Connect(ctx, options.Client().ApplyURI(sourceURI))
	target, _ := mongo.Connect(ctx, options.Client().ApplyURI(targetURI))

	log.Println("Carga inicial...")

	cols, _ := source.Database(dbName).ListCollectionNames(ctx, bson.D{})

	for _, col := range cols {
		sourceCol := source.Database(dbName).Collection(col)
		targetCol := target.Database(dbName).Collection(col)

		cursor, err := sourceCol.Find(ctx, bson.D{})
		if err != nil {
			log.Fatal("erro no find:", err)
		}

		for cursor.Next(ctx) {
			var doc bson.M
			cursor.Decode(&doc)
			targetCol.ReplaceOne(ctx, bson.M{"_id": doc["_id"]}, doc, options.Replace().SetUpsert(true))
		}
	}

	log.Println("Escutando change stream...")

	var resumeToken bson.Raw

	for {
		streamOpts := options.ChangeStream()
		if resumeToken != nil {
			streamOpts.SetResumeAfter(resumeToken)
		}

		stream, err := source.Database(dbName).Watch(ctx, mongo.Pipeline{}, streamOpts)
		if err != nil {
			log.Println("erro ao abrir stream, tentando novamente...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("stream conectado")

		for stream.Next(ctx) {
			var change bson.M
			if err := stream.Decode(&change); err != nil {
				log.Println("erro decode:", err)
				continue
			}

			resumeToken = stream.ResumeToken()

			op := change["operationType"].(string)
			id := change["documentKey"].(bson.M)["_id"]

			ns := change["ns"].(bson.M)
			col := ns["coll"].(string)

			targetCol := target.Database(dbName).Collection(col)

			switch op {
			case "insert", "replace":
				full := change["fullDocument"]
				targetCol.ReplaceOne(ctx, bson.M{"_id": id}, full, options.Replace().SetUpsert(true))

			case "update":
				update := change["updateDescription"].(bson.M)
				targetCol.UpdateOne(ctx,
					bson.M{"_id": id},
					bson.M{"$set": update["updatedFields"]},
				)

			case "delete":
				targetCol.DeleteOne(ctx, bson.M{"_id": id})
			}

			log.Println("evento:", op, col, id)
		}

		log.Println("stream caiu, reconectando...")
		time.Sleep(5 * time.Second)
	}
}
