pipeline {
  agent none
  stages {
    stage('unittest') {
      steps {
        sh 'python -m unittest -v test/test_s3grep.py'
      }
    }
  }
}