#!/bin/bash

# Function to pause script at the end
pause_script() {
    read -p "✅ Setup complete! Press any key to exit..." key
}


echo "🔄 Preparing for airflow ..."
cd DE-Zoomcamp-Project
mkdir -p ./logs ./plugins ./config
cp ~/.gc/google-credential.json ./config 
echo "✅ Preperation Done!"

pause_script