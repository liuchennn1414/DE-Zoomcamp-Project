#!/bin/bash

# Function to handle errors
handle_error() {
    echo "❌ An error occurred! Please check the output above."
    read -p "Press any key to exit..." key
    exit 1
}

# Function to pause script at the end
pause_script() {
    read -p "✅ Setup1 completed! Please proceed with setup2. Press any key to exit..." key
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
echo "Docker has been configured."
newgrp docker

# Final pause before exit
pause_script
