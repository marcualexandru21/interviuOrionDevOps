pipeline {
    agent any

    environment {
        AWS_REGION = 'eu-central-1'
        AWS_ACCOUNT_ID = '216989107224'
        ECR_REPO = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/intervieworion/pyapp"
        IMAGE_TAG = "v${env.BUILD_ID}" // Versionare unica pe baza numarului de build
        EKS_CLUSTER_NAME = 'bootcamp-cluster' 
    }

    stages {
        stage('Checkout') {
            steps {
                // Trage codul folosind credentialele din Jenkins
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    echo "Building image: ${ECR_REPO}:${IMAGE_TAG}"
                    sh "docker build -t ${ECR_REPO}:${IMAGE_TAG} ."
                }
            }
        }

        stage('Push to ECR') {
            steps {
                script {
                    echo "Pushing image..."
                    // FĂRĂ login explicit. Credential helper-ul se ocupa de asta in spate.
                    sh "docker push ${ECR_REPO}:${IMAGE_TAG}"
                }
            }
        }


        stage('Deploy to EKS') {
            steps {
                script {
                    echo "Checking AWS Identity..."
                    sh "aws sts get-caller-identity"

                    echo "Updating Kubeconfig for cluster ${EKS_CLUSTER_NAME}..."
                    sh "aws eks update-kubeconfig --region ${AWS_REGION} --name ${EKS_CLUSTER_NAME}"

                    echo "Injecting image tag into Kubernetes manifest..."
                    // Sed inlocuieste placeholder-ul cu tag-ul real (v1, v2, v3...)
                    sh "sed -i 's/LATEST_TAG_PLACEHOLDER/${IMAGE_TAG}/g' k8s/pyapp-deployment.yaml"

                    echo "Applying manifests..."
                    sh "kubectl apply -f k8s/pyapp-deployment.yaml"

                    echo "Waiting for rollout to finish..."
                    sh "kubectl rollout status deployment/pyapp-deployment -n telecom-apps --timeout=60s"
                }
            }
        }
    }

    post {
        always {
            script {
                echo "Cleaning up local Docker images to prevent disk space exhaustion..."
                // Daca nu faci asta, discul Jenkins-ului se umple in cateva zile in productie
                sh "docker rmi ${ECR_REPO}:${IMAGE_TAG} || true"

                // Stergem credentialele temporare de EKS
                sh "rm -rf ~/.kube/config || true"
            }
        }
        failure {
            echo "Pipeline failed! Check the logs."
        }
    }
}