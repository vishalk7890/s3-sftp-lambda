package main

import (
 "context"
 "encoding/json"
 "fmt"
 "io"
 "log"
 "path/filepath"

 "github.com/aws/aws-lambda-go/lambda"
 "github.com/aws/aws-sdk-go/aws"
 "github.com/aws/aws-sdk-go/aws/session"
 "github.com/aws/aws-sdk-go/service/secretsmanager"
 "github.com/aws/aws-sdk-go/service/s3"
 "github.com/pkg/sftp"
 "golang.org/x/crypto/ssh"
)

const (
 s3Bucket       = "test-sftp-poc-buc"
 s3FolderPrefix = "test-poc"
 region         = "ap-southeast-1"
 secretName     = "sftp-poc"
)

type SFTPConfig struct {
 SFTPHost     string `json:"sftpHost"`
 SFTPPort     string `json:"sftpPort"`
 SFTPUsername string `json:"sftpUsername"`
 SFTPPassword string `json:"sftpPassword"`
}

func main() {
 lambda.Start(lambdaHandler)
}

func lambdaHandler(ctx context.Context) error {
 log.Println("Lambda handler started")

 log.Println("Creating new AWS session")
 sess, err := session.NewSession(&aws.Config{
  Region: aws.String(region),
 })
 if err != nil {
  log.Printf("Failed to create AWS session: %v", err)
  return fmt.Errorf("failed to create AWS session: %w", err)
 }
 log.Println("AWS session created")

 sftpConfig, err := getSFTPConfig(sess)
 if err != nil {
  log.Printf("Failed to get SFTP config: %v", err)
  return fmt.Errorf("failed to get SFTP config: %w", err)
 }

 svc := s3.New(sess)

 // List objects in the specified folder
 log.Println("Listing objects in S3 bucket")
 resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
  Bucket: aws.String(s3Bucket),
  Prefix: aws.String(s3FolderPrefix),
 })
 if err != nil {
  log.Printf("Failed to list objects: %v", err)
  return fmt.Errorf("failed to list objects: %w", err)
 }

 for _, item := range resp.Contents {
  key := *item.Key
  log.Printf("Found object: %s", key)
  if !isDirectory(key) { // Skip directories
   err := copyObjectToSFTP(svc, key, sftpConfig)
   if err != nil {
    log.Printf("Failed to copy file to SFTP: %v", err)
    return fmt.Errorf("failed to copy file to SFTP: %w", err)
   }
  }
 }

 log.Println("Files transferred successfully!")
 return nil
}

func isDirectory(key string) bool {
 return key[len(key)-1] == '/'
}

func getSFTPConfig(sess *session.Session) (*SFTPConfig, error) {
 svc := secretsmanager.New(sess)
 input := &secretsmanager.GetSecretValueInput{
  SecretId: aws.String(secretName),
 }
 result, err := svc.GetSecretValue(input)
 if err != nil {
  return nil, fmt.Errorf("failed to retrieve secret: %w", err)
 }

 var sftpConfig SFTPConfig
 err = json.Unmarshal([]byte(*result.SecretString), &sftpConfig)
 if err != nil {
  return nil, fmt.Errorf("failed to unmarshal secret: %w", err)
 }

 return &sftpConfig, nil
}

func copyObjectToSFTP(svc *s3.S3, key string, sftpConfig *SFTPConfig) error {
 sshConfig := &ssh.ClientConfig{
  User: sftpConfig.SFTPUsername,
  Auth: []ssh.AuthMethod{
   ssh.Password(sftpConfig.SFTPPassword),
  },
  HostKeyCallback: ssh.InsecureIgnoreHostKey(),
 }

 address := fmt.Sprintf("%s:%s", sftpConfig.SFTPHost, sftpConfig.SFTPPort)
 log.Println("Dialing SFTP server:", address)
 conn, err := ssh.Dial("tcp", address, sshConfig)
 if err != nil {
  log.Printf("Failed to dial SFTP server: %v", err)
  return fmt.Errorf("failed to dial: %w", err)
 }
 defer conn.Close()
 log.Println("SFTP connection established")

 sftpClient, err := sftp.NewClient(conn)
 if err != nil {
  log.Printf("Failed to create SFTP client: %v", err)
  return fmt.Errorf("failed to create SFTP client: %w", err)
 }
 defer sftpClient.Close()

 log.Printf("Copying S3 object %s to SFTP", key)
 getObjectOutput, err := svc.GetObject(&s3.GetObjectInput{
  Bucket: aws.String(s3Bucket),
  Key:    aws.String(key),
 })
 if err != nil {
  log.Printf("Failed to get S3 object: %v", err)
  return fmt.Errorf("failed to get S3 object: %w", err)
 }
 defer getObjectOutput.Body.Close()

 remoteFilePath := fmt.Sprintf("/uploads/%s", filepath.Base(key))
 remoteDir := filepath.Dir(remoteFilePath)

 // Ensure the directory exists
 log.Printf("Ensuring directory exists: %s", remoteDir)
 err = sftpClient.MkdirAll(remoteDir)
 if err != nil {
  log.Printf("Failed to create remote directory: %v", err)
  return fmt.Errorf("failed to create remote directory: %w", err)
 }

 dstFile, err := sftpClient.Create(remoteFilePath)
 if err != nil {
  log.Printf("Failed to create remote file: %v", err)
  return fmt.Errorf("failed to create remote file: %w", err)
 }
 defer dstFile.Close()

 log.Printf("Transferring data to %s", remoteFilePath)
 _, err = io.Copy(dstFile, getObjectOutput.Body)
 if err != nil {
  log.Printf("Failed to copy file to remote: %v", err)
  return fmt.Errorf("failed to copy file to remote: %w", err)
 }

 log.Printf("File transferred successfully to %s", remoteFilePath)
 return nil
}
