## build cmd

<code>
docker build -t mini-proj .
</code>

## run cmd

<code>
docker run --rm --name gh-api -e GITHUB_BEARER_TOKEN mini-proj
</code>

# Venv Setup

## create venv

<code>
virtualenv venv
</code>

## activate

<code>
source venv/bin/activate
</code>

## install deps
<code>
pip install -r requirements.txt
</code>

## deactivate
<code>
deactivate
</code>