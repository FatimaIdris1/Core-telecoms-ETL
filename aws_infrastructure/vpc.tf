resource "aws_vpc" "coretelecomms-vpc" {
  cidr_block       = "10.0.0.0/16"
  instance_tenancy = "default"

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "Coretelecomms pipeline"
  }
}

resource "aws_internet_gateway" "gateway" {
  vpc_id = aws_vpc.coretelecomms-vpc.id


  tags = {
    Name = "Coretelecomms pipeline"
  }
}


# Route table for public subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.coretelecomms-vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gateway.id
  }

  tags = {
    Name = "Coretelecomms Public Route Table"
  }
}

resource "aws_route_table_association" "db_subnet_1" {
  subnet_id      = aws_subnet.subnet_avail-zone1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "db_subnet_2" {
  subnet_id      = aws_subnet.subnet_avail-zone2.id
  route_table_id = aws_route_table.public.id
}