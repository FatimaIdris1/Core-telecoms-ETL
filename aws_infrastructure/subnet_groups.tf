resource "aws_db_subnet_group" "ctp_db_subnet_group" {
  name       = "rds_subnet_group"
  subnet_ids = [aws_subnet.subnet_avail-zone1.id, aws_subnet.subnet_avail-zone2.id]

  tags = {
    Name = "CoreTelecomms database subnet group"
  }
}