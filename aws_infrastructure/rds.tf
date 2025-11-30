resource "aws_db_instance" "coretelecomms_db_instance" {
  allocated_storage           = 100
  db_name                     = "coretelecomms"
  engine                      = "postgres"
  port                        = 5432
  engine_version              = "16.11"
  instance_class              = "db.r5.large"
  multi_az                    = false
  db_subnet_group_name        = aws_db_subnet_group.ctp_db_subnet_group.name
  vpc_security_group_ids      = [aws_security_group.database_security_group.id]
  username                    = "telecomms_user"
  manage_master_user_password = true
  skip_final_snapshot         = true  # TODO: change to false in prod
  publicly_accessible         = true  # TODO: change to false AFTER AIRFLOW DEPLOYMENT

  tags = {
    Name        = "CoreTelecoms database"
    Environment = "Development"
  }
}

 # skip_final_snapshot         = false  
 #  final_snapshot_identifier   = "telecomms-db-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"