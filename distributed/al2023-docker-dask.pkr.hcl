packer {
  required_plugins {
    amazon = {
      version = ">= 1.0.0"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "ami_owner_id" {
  type    = string
  default = ""
}

variable "vpc_id" {
  type    = string
  default = ""
}

variable "subnet_id" {
  type    = string
  default = ""
}

source "amazon-ebs" "al2023-docker-dask" {
  ami_name      = "al2023-docker-dask-{{timestamp}}"
  instance_type = "t3.medium"
  region        = "us-east-1"
  vpc_id        = var.vpc_id
  subnet_id     = var.subnet_id
  source_ami_filter {
    filters = {
      name                = "edc-app-base-25.1.3.0-al2023-x86_64"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = [var.ami_owner_id]
  }
  ssh_username = "ec2-user"
  ssh_interface = "session_manager"
  communicator = "ssh"
  iam_instance_profile = "Packer-SSM-role"
}

build {
  name = "amazon-linux-2023-docker-dask"
  sources = [
    "source.amazon-ebs.al2023-docker-dask"
  ]

  provisioner "shell" {
    remote_folder = "/home/ec2-user"
    inline = [
      "sudo dnf update -y",
      "sudo dnf install -y docker",
      "sudo systemctl enable docker",
      "sudo systemctl start docker",
      "sudo usermod -aG docker ec2-user",
      "sudo docker pull daskdev/dask:latest",
      "sudo mkdir /mnt/efs"
    ]
  }

  post-processor "manifest" {
    output = "manifest.json"
    strip_path = true
  }
}
