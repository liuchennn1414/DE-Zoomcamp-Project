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
