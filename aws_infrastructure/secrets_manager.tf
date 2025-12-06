resource "aws_secretsmanager_secret" "google_cloud_secrets" {
 name = "google_cloud_credv2"
}

resource "aws_secretsmanager_secret_version" "google_cloud_secrets_version" {
  secret_id     = aws_secretsmanager_secret.google_cloud_secrets.id
  secret_string = file("/home/fatima/Downloads/service_account.json")
}
