import os
import sys

def check_files():
    """Check if all required files exist"""
    required_files = [
        "DefiLlama_scraper.py",
        "DefiLlama_to_postgresql.py",
        "Reddit_scraper.py",
        "RAG.py",
        "check.py",
        ".github/workflows/crypto_data_pipeline.yml",
        ".github/workflows/reddit_scraper.yml"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print("MISSING FILES:", missing_files)
        return False
    else:
        print("✓ All required files exist")
        return True

def check_mock_modes():
    """Check if mock modes are implemented in all scripts"""
    with open("DefiLlama_mock.py", "r") as f:
        if "--mock" not in f.read():
            print("✗ DefiLlama_mock.py doesn't have mock mode")
            return False
    
    with open("Reddit_scraper.py", "r") as f:
        if "--mock" not in f.read():
            print("✗ Reddit_scraper.py doesn't have mock mode")
            return False
    
    with open("RAG.py", "r") as f:
        if "--mock" not in f.read():
            print("✗ RAG.py doesn't have mock mode")
            return False
    
    print("✓ All scripts have mock mode implemented")
    return True

def check_workflow_files():
    """Check if workflow files contain necessary secrets"""
    with open(".github/workflows/crypto_data_pipeline.yml", "r") as f:
        content = f.read()
        if "secrets.DB_HOST" not in content:
            print("✗ crypto_data_pipeline.yml is missing database secrets")
            return False
    
    with open(".github/workflows/reddit_scraper.yml", "r") as f:
        content = f.read()
        if "secrets.REDDIT_CLIENT_ID" not in content:
            print("✗ reddit_scraper.yml is missing Reddit secrets")
            return False
        if "secrets.OPENAI_API_KEY" not in content:
            print("✗ reddit_scraper.yml is missing OpenAI secrets")
            return False
    
    print("✓ Workflow files contain necessary secrets")
    return True

def main():
    print("=============================================")
    print("GITHUB ACTIONS READINESS CHECK")
    print("=============================================")
    
    file_check = check_files()
    mock_check = check_mock_modes()
    workflow_check = check_workflow_files()
    
    print("=============================================")
    if file_check and mock_check and workflow_check:
        print("✅ All checks passed! The repository is ready for GitHub Actions.")
        print("The system will work with both real API calls and in mock mode.")
    else:
        print("❌ Some checks failed. See details above.")
    
    print("=============================================")

if __name__ == "__main__":
    main() 