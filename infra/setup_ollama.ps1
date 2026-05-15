Write-Host "============================================="
Write-Host " Phase 4: Local AI Engine Setup (Ollama)    "
Write-Host "============================================="
Write-Host "Since you have an AMD Radeon RX 9070 XT, you need Ollama with ROCm support."
Write-Host "Ollama on Windows currently supports AMD GPUs natively using ROCm."
Write-Host ""
Write-Host "Step 1: Download and install Ollama for Windows from https://ollama.com/download/OllamaSetup.exe"
Write-Host "Step 2: Ensure you have the latest AMD Adrenalin Edition drivers installed."
Write-Host "Step 3: Run the installer."
Write-Host ""
Write-Host "Press any key once Ollama is installed and running, or press Ctrl+C to exit..."
$Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") | Out-Null

Write-Host "`nPulling llama3.2..."
ollama pull llama3.2

Write-Host "`nPulling nomic-embed-text..."
ollama pull nomic-embed-text

Write-Host "`nAll models pulled successfully!"
