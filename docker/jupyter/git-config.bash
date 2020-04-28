#!/usr/bin/env bash

GIT_CONFIG=~/.gitconfig

read_git_fullname() {
  echo -n "Enter Full name: "
  read GIT_FULL_NAME
}

read_git_email() {
  echo -n "Enter Email: "
  read GIT_EMAIL
}

read_git_user() {
  echo -n "Enter Git Username: "
  read GIT_USERNAME
}

read_git_password() {
  echo -n "Enter Git Personal access token (password): "
  read -s GIT_PASSWORD
}

read_git_fullname
read_git_email
read_git_user
#read_git_password

cat > $GIT_CONFIG << EOF
[user]
        name = $GIT_FULL_NAME
        email = $GIT_EMAIL
[credential]
        username = $GIT_USERNAME
[core]
        autocrlf = input
[diff "jupyternotebook"]
        command = git-nbdiffdriver diff
[merge "jupyternotebook"]
        driver = git-nbmergedriver merge %O %A %B %L %P
        name = jupyter notebook merge driver
[difftool "nbdime"]
        cmd = git-nbdifftool diff \"$LOCAL\" \"$REMOTE\" \"$BASE\"
[difftool]
        prompt = false
[mergetool "nbdime"]
        cmd = git-nbmergetool merge \"$BASE\" \"$LOCAL\" \"$REMOTE\" \"$MERGED\"
[mergetool]
        prompt = false
EOF

echo "Successfully configured your Git Account!"
