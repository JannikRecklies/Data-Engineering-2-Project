import os
from github import Github, InputGitAuthor
from subprocess import run, CalledProcessError
from pulsar import Client, Message

# Constants
REPO_NAME = 'rajib13myself/https://github.com/JannikRecklies/Data-Engineering-2-Project/'
BRANCH_NAME = 'rd-commit'
USERNAME = 'rd-proj'
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
COMMIT_MESSAGE = f'Update {CURRENT_DATE} by {USERNAME}'
PR_TITLE = f'Update {CURRENT_DATE}'
PR_BODY = f'Changes made on {CURRENT_DATE}. changes by {USERNAME}'
GITHUB_TOKEN = os.getenv('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')

# Initialize GitHub instance
g = Github(GITHUB_TOKEN)
repo = g.get_repo(REPO_NAME)

# Function to run shell commands
def run_command(command):
    try:
        result = run(command, check=True, capture_output=True, text=True, shell=True)
        return result.stdout.strip()
    except CalledProcessError as e:
        print(f"Error running command '{command}': {e.stderr}")
        return None

# Create a new branch
def create_branch(branch_name):
    print(f"Creating branch {branch_name}...")
    run_command(f'git checkout -b {branch_name}')

# Stage changes
def stage_changes():
    print("Staging changes...")
    run_command('git add .')

# Commit changes
def commit_changes(commit_message):
    print("Committing changes...")
    author = InputGitAuthor(
        name="rajib13myself",
        email="rajib43.datta@gamil.com"
    )
    run_command(f'git commit -m "{commit_message}"')

# Push branch
def push_branch(branch_name):
    print(f"Pushing branch {branch_name}...")
    run_command(f'git push origin {branch_name}')

# Create pull request
def create_pull_request(title, body, head, base='main'):
    print("Creating pull request...")
    pr = repo.create_pull(
        title=title,
        body=body,
        head=head,
        base=base
    )
    print(f"Pull request created: {pr.html_url}")

# Delete branch locally and remotely
def delete_branch(branch_name):
    print(f"Deleting branch {branch_name} locally...")
    run_command(f'git branch -d {branch_name}')
    print(f"Deleting branch {branch_name} remotely...")
    run_command(f'git push origin --delete {branch_name}')

# Pulsar consumer function
def pulsar_consumer():
    client = Client('pulsar://localhost:6650')
    consumer = client.subscribe('my-topic', 'my-subscription')
    while True:
        msg = consumer.receive()
        try:
            print(f"Received message: {msg.data()}")
            # Process the message here
            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Error processing message: {e}")
            consumer.negative_acknowledge(msg)
    client.close()

# Pulsar producer function
def pulsar_producer():
    client = Client('pulsar://localhost:6650')
    producer = client.create_producer('my-topic')
    while True:
        message = input("Enter message to send (or 'exit' to quit): ")
        if message == 'exit':
            break
        producer.send(message.encode('utf-8'))
    client.close()

# Main script
if __name__ == "__main__":
    # Uncomment one of the following lines based on whether you want to be a producer or consumer
    # pulsar_consumer()
    # pulsar_producer()

