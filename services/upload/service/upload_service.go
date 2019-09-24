package service

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/HackIllinois/api/common/database"
	"github.com/HackIllinois/api/services/upload/config"
	"github.com/HackIllinois/api/services/upload/models"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var db database.Database

var sess *session.Session
var client *s3.S3

func Initialize() error {
	sess = session.Must(session.NewSession(&aws.Config{
		Region: aws.String(config.S3_REGION),
	}))
	client = s3.New(sess)

	if db != nil {
		db.Close()
		db = nil
	}

	var err error
	db, err = database.InitDatabase(config.UPLOAD_DB_HOST, config.UPLOAD_DB_NAME)

	if err != nil {
		return err
	}

	return nil
}

/*
	Returns a presigned link to user requested user's resume
*/
func GetUserResumeLink(id string) (*models.UserResume, error) {
	var signed_url string
	var err error

	if config.IS_PRODUCTION {
		request, _ := client.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(config.S3_BUCKET),
			Key:    aws.String("resumes/" + id + ".pdf"),
		})

		signed_url, err = request.Presign(15 * time.Minute)

		if err != nil {
			return nil, err
		}
	} else {
		signed_url = "/tmp/uploads/" + id + ".pdf"
	}

	resume := models.UserResume{
		ID:     id,
		Resume: signed_url,
	}

	return &resume, nil
}

/*
	Update the given user's resume
*/
func GetUpdateUserResumeLink(id string) (*models.UserResume, error) {
	var signed_url string
	var err error

	if config.IS_PRODUCTION {
		request, _ := client.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(config.S3_BUCKET),
			Key:    aws.String("resumes/" + id + ".pdf"),
		})

		signed_url, err = request.Presign(15 * time.Minute)

		if err != nil {
			return nil, err
		}
	} else {
		signed_url = "/tmp/uploads/" + id + ".pdf"
	}

	resume := models.UserResume{
		ID:     id,
		Resume: signed_url,
	}

	return &resume, nil
}

/*
	Returns the blob with the given id
*/
func GetBlob(id string) (*models.Blob, error) {
	query := database.QuerySelector{
		"id": id,
	}

	var blob models.Blob
	err := db.FindOne("blobstore", query, &blob)

	if err != nil {
		return nil, err
	}

	return &blob, nil
}

/*
	Creates and stores a blob
*/
func CreateBlob(blob models.Blob) error {
	_, err := GetBlob(blob.ID)

	if err != database.ErrNotFound {
		if err != nil {
			return err
		}
		return errors.New("Blob already exists.")
	}

	err = db.Insert("blobstore", &blob)

	return err
}

/*
	Updates the blob with the given id
*/
func UpdateBlob(blob models.Blob) error {
	selector := database.QuerySelector{
		"id": blob.ID,
	}

	err := db.Update("blobstore", selector, &blob)

	return err
}

/*
	Update partial data of blob with given id.
	This function convert partial blob data into full blob data with partial values changed, and then call database update
*/
func UpdatePartialBlob(blob models.Blob) error {

	// first get the blob with id given to check if it exists
	blob_full, err_get := GetBlob(blob.ID)
	if err_get != nil {
		return errors.New("Blob does not exist.")
	}

	// create a map of full data for this existed blob (create map so that we can loop through keys afterwards)
	blob_full_data := map[string]interface{}{}
	json1, err1 := json.Marshal(blob_full.Data)
	if err1 != nil {
		return err1
	}
	json.Unmarshal([]byte(json1), &blob_full_data)

	// create a map of partial data for the blob that we get from argument
	blob_data := map[string]interface{}{}
	json2, err2 := json.Marshal(blob.Data)
	if err2 != nil {
		return err2
	}
	json.Unmarshal([]byte(json2), &blob_data)

	// iterate over the keys in blob data so that we can update partial values of the full_data
	for key := range blob_data {
		blob_full_data[key] = blob_data[key]
	}

	// create a new blob with full data that we updated
	blob_data_update := models.Blob{
		ID:   blob.ID,
		Data: blob_full_data,
	}

	// call database function Update to update existed blob
	selector := database.QuerySelector{
		"id": blob.ID,
	}
	err := db.Update("blobstore", selector, &blob_data_update)

	return err
}
