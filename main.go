package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

const (
	jeopordyBucket = "questions"
)

// Question defines Jeopardy questions
type Question struct {
	Category   string `json:"category"`
	AirDate    string `json:"air_date"`
	Question   string `json:"question"`
	Value      string `json:"value"`
	Answer     string `json:"answer"`
	Round      string `json:"round"`
	ShowNumber string `json:"show_number"`
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func loadJeopardyFromFile() []Question {
	file, err := os.Open("jeopardy_questions.json")
	if err != nil {
		fmt.Println("file read err: ", err)
		log.Fatal(err)
	}
	defer file.Close()

	var questions []Question
	body, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("io err: ", err)
		log.Fatal(err)
	}

	err = json.Unmarshal(body, &questions)
	if err != nil {
		fmt.Println("unmarshal err:", err)
		return []Question{}
	}
	return questions
}

func saveQuestions(db *bolt.DB, questions []Question) {
	i := 0
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(jeopordyBucket))
		if err != nil {
			return fmt.Errorf("could not create questions bucket")
		}
		for _, q := range questions {
			buf, err := json.Marshal(q)
			if err != nil {
				fmt.Println("marshal err:", err)
				return err
			}
			err = b.Put(itob(i), buf)
			if err != nil {
				return err
			}
			i++
		}
		return err
	})
	if err != nil {
		return
	}
	fmt.Printf("\nSuccessfully saved %d questions\n", i)
	return
}

func printQuestionByKey(db *bolt.DB, n int) {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(jeopordyBucket))
		v := b.Get([]byte(itob(n)))
		fmt.Printf("Question from DB: %s\n", v)
		return nil
	})
	if err != nil {
		return
	}
}

func main() {
	db, err := bolt.Open("datacastle.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := loadJeopardyFromFile()

	fmt.Printf("Questions=%d", len(q))

	saveQuestions(db, q)

	printQuestionByKey(db, 1)
}
