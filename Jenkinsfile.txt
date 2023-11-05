pipeline {
  agent any
  environment {
    CLOUDSDK_CORE_PROJECT='secret-country-401617'
    CLIENT_EMAIL='jenkins-inceptez@secret-country-401617.iam.gserviceaccount.com'
    GCLOUD_CREDS=credentials('sa-inceptez-key')
  }
  stages {
    stage('copy_to_gcp') {
      steps {
        sh '''
          ls -lrt
          gcloud auth activate-service-account --key-file="$GCLOUD_CREDS"
          gsutil cp *.py gs://inceptez-pyspark/
          echo 'File Copied into GCS bucket'
        '''
      }
    }
  }
  post {
    always {
      sh 'gcloud auth revoke $CLIENT_EMAIL'
    }
  }
}
