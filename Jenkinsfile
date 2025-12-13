pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        ECR_REGISTRY = '664418964913.dkr.ecr.us-east-1.amazonaws.com'
        EKS_CLUSTER = 'imeetpro-eks-prod'
        FRONTEND_REPO = "${ECR_REGISTRY}/imeetpro-frontend"
        BACKEND_REPO = "${ECR_REGISTRY}/imeetpro-backend"
    }
    
    stages {
        stage('Clone Application Code') {
            steps {
                git branch: 'main', url: 'https://github.com/anumularoots-svg/iMeetPro.git'
            }
        }
        
        stage('ECR Login') {
            steps {
                sh '''
                    aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}
                '''
            }
        }
        
        stage('Build Backend Image') {
            steps {
                dir('iMeet-backend') {
                    sh '''
                        docker build -t ${BACKEND_REPO}:${BUILD_NUMBER} -t ${BACKEND_REPO}:latest .
                    '''
                }
            }
        }
        
        stage('Build Frontend Image') {
            steps {
                dir('iMeet-frontend') {
                    sh '''
                        docker build -t ${FRONTEND_REPO}:${BUILD_NUMBER} -t ${FRONTEND_REPO}:latest .
                    '''
                }
            }
        }
        
        stage('Push to ECR') {
            steps {
                sh '''
                    docker push ${BACKEND_REPO}:${BUILD_NUMBER}
                    docker push ${BACKEND_REPO}:latest
                    docker push ${FRONTEND_REPO}:${BUILD_NUMBER}
                    docker push ${FRONTEND_REPO}:latest
                '''
            }
        }
        
        stage('Configure Kubectl') {
            steps {
                sh '''
                    aws eks update-kubeconfig --region ${AWS_REGION} --name ${EKS_CLUSTER}
                '''
            }
        }
        
        stage('Deploy to EKS') {
            steps {
                sh '''
                    rm -rf iMeetPro-Infrastructure || true
                    git clone https://github.com/anumularoots-svg/iMeetPro-Infrastructure.git
                    cd iMeetPro-Infrastructure/k8s
                    
                    kubectl apply -f namespace.yaml
                    kubectl apply -f configmap.yaml
                    kubectl apply -f secrets.yaml
                    kubectl apply -f backend-deployment.yaml
                    kubectl apply -f frontend-deployment.yaml
                    kubectl apply -f ingress.yaml
                '''
            }
        }
        
        stage('Restart Deployments') {
            steps {
                sh '''
                    kubectl rollout restart deployment/imeetpro-backend -n imeetpro-prod
                    kubectl rollout restart deployment/imeetpro-frontend -n imeetpro-prod
                '''
            }
        }
        
        stage('Verify') {
            steps {
                sh '''
                    echo "=== Pods Status ==="
                    kubectl get pods -n imeetpro-prod
                    echo "=== Services ==="
                    kubectl get svc -n imeetpro-prod
                '''
            }
        }
    }
    
    post {
        success {
            echo '✅ Deployment Successful! Access: https://imeetpro.lancieretech.com'
        }
        failure {
            echo '❌ Deployment Failed! Check logs.'
        }
    }
}
