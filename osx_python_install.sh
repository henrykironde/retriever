#!/bin/bash -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update || brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv

    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi

    pyenv install $PYTHON_VERSION
    pyenv virtualenv $PYTHON_VERSION virtualenv
    pyenv rehash
    pyenv activate virtualenv
fi
