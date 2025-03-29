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

# Add user to Docker group
echo "🔧 Testing Connection..."
sudo service docker restart || handle_error

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
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc || handle_error
echo "✅ PATH updated!"

# Download Terraform 
echo "Download Terraform..."
cd $HOME/bin 
wget https://releases.hashicorp.com/terraform/1.1.3/terraform_1.1.3_linux_amd64.zip 
sudo apt-get install unzip 
unzip terraform_1.1.3_linux_amd64.zip 
rm terraform_1.1.3_linux_amd64.zip 
chmod +x $HOME/bin/terraform || handle_error
echo "✅ Terraform installed!"


# Final pause before exit
pause_script
