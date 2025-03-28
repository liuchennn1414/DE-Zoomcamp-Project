#!/bin/bash

# Function to handle errors
handle_error() {
    echo "❌ An error occurred! Please check the output above."
    read -p "Press any key to exit..." key
    exit 1
}

# Function to pause script at the end
pause_script() {
    read -p "✅ Setup complete! Press any key to exit..." key
}

# # Download and install Anaconda (not needed)
# echo "🚀 Downloading Anaconda..."
# wget https://repo.anaconda.com/archive/Anaconda3-2024.10-1-Linux-x86_64.sh || handle_error
# bash Anaconda3-2024.10-1-Linux-x86_64.sh || handle_error
# echo "✅ Anaconda download complete!"

# source .bashrc || handle_error
# echo "✅ Anaconda activated!"

# Install Docker
echo "🐳 Installing Docker..."
sudo apt-get update -y && sudo apt-get install -y docker.io || handle_error
echo "✅ Docker installed!"

# Add user to Docker group
echo "🔧 Configuring Docker..."
sudo gpasswd -a $USER docker || handle_error
sudo usermod -aG docker $USER || handle_error
newgrp docker || handle_error 
sudo service docker restart || handle_error
echo "🔧 Testing Connection..."
docker run hello-world || handle_error
echo "✅ Docker configured!"

# Install Docker Compose
echo "📦 Installing Docker Compose..."
mkdir -p $HOME/bin
wget -q https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O $HOME/bin/docker-compose || handle_error
chmod +x $HOME/bin/docker-compose || handle_error
echo "✅ Docker Compose installed!"

# Update PATH
echo "🔄 Updating PATH..."
echo 'export PATH="${HOME}/bin:${PATH}"' >> ~/.bashrc
source .bashrc || handle_error
echo "✅ PATH updated!"

# Download Terraform 
echo "Download Terraform..."
cd bin 
wget https://releases.hashicorp.com/terraform/1.1.3/terraform_1.1.3_linux_amd64.zip 
sudo apt-get install unzip 
unzip terraform_1.1.3_linux_amd64.zip 
rm terraform_1.1.3_linux_amd64.zip 
echo "✅ Terraform installed!"


# Final pause before exit
pause_script
