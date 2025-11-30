data "aws_availability_zones" "available_zones" {}


resource "aws_subnet" "subnet_avail-zone1" {
  vpc_id            = aws_vpc.coretelecomms-vpc.id
  cidr_block        = "10.0.0.0/24"
  availability_zone = data.aws_availability_zones.available_zones.names[0]

}

resource "aws_subnet" "subnet_avail-zone2" {
  vpc_id            = aws_vpc.coretelecomms-vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available_zones.names[1]

}



