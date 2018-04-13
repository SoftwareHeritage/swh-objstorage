pipeline {
    agent none
    environment {
        PYTHON_VERSION = '3.5'
    }
    stages {
        stage('Lint') {
            agent {
                dockerfile {
                    filename 'dockerfiles/Dockerfile.lint'
                    additionalBuildArgs '--build-arg python_version=${env.PYTHON_VERSION}'
                }
            }
            steps {
                sh 'python3 -m flake8 swh'
            }
        }
    }
}
