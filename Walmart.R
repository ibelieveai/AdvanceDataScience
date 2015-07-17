rm(list=ls())
setwd("C:/Users/Bellamkonda/Desktop/DataScience/Walmart")
dfStore <- read.csv(file="C:/Users/Bellamkonda/Desktop/DataScience/Walmart/stores.csv")
dfTrain <- read.csv(file="C:/Users/Bellamkonda/Desktop/DataScience/Walmart/train.csv")
dfTest <- read.csv(file="C:/Users/Bellamkonda/Desktop/DataScience/Walmart/test.csv")
dfFeatures <- read.csv(file="C:/Users/Bellamkonda/Desktop/DataScience/Walmart/features.csv")
submission = read.csv(file="C:/Users/Bellamkonda/Desktop/DataScience/Walmart/sampleSubmission.csv",header=TRUE,as.is=TRUE)

# Merge Type and Size
dfTrainTmp <- merge(x=dfTrain, y=dfStore, all.x=TRUE)
dfTestTmp <- merge(x=dfTest, y=dfStore, all.x=TRUE)

# Merge all the features
train <- merge(x=dfTrainTmp, y=dfFeatures, all.x=TRUE)
test <- merge(x=dfTestTmp, y=dfFeatures, all.x=TRUE)

write.csv(train, file = "MergedTrain1.csv")
write.csv(test, file = "MergedTest.csv")

# Make features for train
train$year = as.numeric(substr(train$Date,1,4))
train$month = as.numeric(substr(train$Date,6,7))
train$day = as.numeric(substr(train$Date,9,10))

###PREPROCESSING 
##Removing all NAs
Data<-train[complete.cases(train),]
b<-which(is.na(Data)==TRUE)
length(b)
trainiday<-data.frame(Data)


###Imputing with local means
M1_Avg<-means1$MarkDown1
M2_Avg<-means1$MarkDown2
M3_Avg<-means1$MarkDown3
M4_Avg<-means1$MarkDown4
M5_Avg<-means1$MarkDown5

for(j in 1:nrow(train)){
if (is.na(train$MarkDown1[j])){
train$MarkDown1<-M1_Avg[train$Store]
}
}
for(j in 1:nrow(train)){
if (is.na(train$MarkDown2[j])){
train$MarkDown2<-M2_Avg[train$Store]
}
}
for(j in 1:nrow(train)){
if (is.na(train$MarkDown3[j])){
train$MarkDown3<-M3_Avg[train$Store]
}
}
for(j in 1:nrow(train)){
if (is.na(train$MarkDown4[j])){
train$MarkDown4<-M4_Avg[train$Store]
}
}
for(j in 1:nrow(train)){
if (is.na(train$MarkDown5[j])){
train$MarkDown5<-M5_Avg[train$Store]
}
}
##GLOBAL MEAN IMPUTATION

##Identifying the NAs
y<-which(is.na(train$MarkDown1)==TRUE)
y1<-which(is.na(train$MarkDown2)==TRUE)
y2<-which(is.na(train$MarkDown3)==TRUE)
y3<-which(is.na(train$MarkDown4)==TRUE)
y4<-which(is.na(train$MarkDown5)==TRUE)

##Imputing the global means in the NAs place
Data[,11][y]<-"7246.4"
Data[,12][y1]<-"3334"
Data[,13][y2]<-"1439.4"
Data[,14][y3]<-"3383.1"
Data[,15][y4]<-"4628.9"
##Checking for NAs
b<-which(is.na(Data)==TRUE)


##Transformation
Data$Type<-as.character(Data$Type)
A<-which(Data$Type=='A')
B<-which(Data$Type=='B')
C<-which(Data$Type=='C')
Data$Type[A]<-1
Data$Type[B]<-2
Data$Type[C]<-3
Data$Type<-as.numeric(as.character(Data$Type))
x <- as.POSIXct(Data[,3])
Data[,3]<-as.numeric(x)
X1<-Data[,-1]

#ModelMatrix for dummy coding type
X<-model.matrix(Weekly_Sales~Store+Dept+Type+Size+TeDatamperature+Fuel_Price
+MarkDown1+MarkDown2+MarkDown3+MarkDown4+MarkDown5+CPI
+Unemployment+factor(Week)+factor(Month)+factor(Year)+factor(IsHoliday),data=Data)
X1<-cbind(X[,2:ncol(X)],Data$Weekly_Sales)
X1<-data.frame(X1)

##Dividing into test and train 
indexes<- sample(1:nrow(X1),size=0.3*nrow(X1))
train<-X1[-indexes,]
test<-X1[indexes,]

##45 LOCAL MODELS BY STORE
MSE_test<-matrix(data=NA,nrow=45,ncol=1)
MSE_train<-matrix(data=NA,nrow=45,ncol=1)
Rsquare<-matrix(data=NA,nrow=45,ncol=1)
Size<-matrix(data=NA,nrow=45,ncol=2)
for( key in 1:45){
lm.train <- subset(train, Store==key)
lm.test <- subset(test, Store==key)
Size[key,1]<-nrow(lm.train)
Size[key,2]<-nrow(lm.test)
# build a linear model with train
#Linear-Model
Linear_Local<-lm(formula=Weekly_Sales~.,data=lm.train)
# predict on test, and measure MSE
lm.test$pred <- NULL
lm.test$pred<-as.matrix(predict(Linear_Local,lm.test))
lm.train$pred <- NULL
lm.train$pred<-as.matrix(predict(Linear_Local,lm.train))
MSE_train[key,1]<-mean((lm.train$Weekly_Sales-lm.train$pred)^2)
MSE_test[key,1]<-mean((lm.test$Weekly_Sales-lm.test$pred)^2)
Rsquare[key,1]<-getR2(lm.test$pred,lm.test$Weekly_Sales)
key<-key+1
}

RMSE_test<-sqrt(MSE_test)
RMSE_test_LOCAL<-mean(RMSE_test)
RMSE_train<-sqrt(MSE_train)
RMSE_train_LOCAL<-mean(RMSE_train)
RMSE_train_LOCAL
RMSE_test_LOCAL
Rsquare_final<-mean(Rsquare)


### 3 LOCAL MODELS BY TYPE 
MSE_test<-matrix(data=NA,nrow=3,ncol=1)
MSE_train<-matrix(data=NA,nrow=3,ncol=1)
Rsquare_train<-matrix(data=NA,nrow=3,ncol=1)
Rsquare_test<-matrix(data=NA,nrow=3,ncol=1)
Size<-matrix(data=NA,nrow=3,ncol=2)
for( key in 1:3){
lm.train <- subset(train, Type==key)
lm.test <- subset(test, Type==key)
Size[key,1]<-nrow(lm.train)
Size[key,2]<-nrow(lm.test)
# build a linear model with train
#Linear-Model
Linear_Local<-lm(formula=Weekly_Sales~.,data=lm.train)


###MODELLING 
##Linear Flat Model
Linear_Flat<-lm(Weekly_Sales~factor(Store)+factor(Date)+factor(IsHoliday)+
factor(Dept)+factor(Type)+Temperature+Fuel_Price+MarkDown2
+MarkDown3+MarkDown4+MarkDown5+CPI+Unemployment+Week+Month+Year, data=train)

##Predictions
train$predLM<-predict(Linear_Flat,data=train)
test$predLM<-predict(Linear_Flat,test)
#Results
train_RMSE_LM<-getRMSE(train$predLM,train$Weekly_Sales)
train_Rsquare_LM<-getR2(train$predLM,train$Weekly_Sales)
b<-which(is.na(test$predLM)==TRUE)
test_RMSE_LM<-getRMSE(test$predLM,test$Weekly_Sales)
test_Rsquare_LM<-getR2(test$predLM,test$Weekly_Sales)


##MULTI LEVEL MODELLING :
library(nlme)
###VARYING INTERCEPT 
MLMStoreDept<-lme(Weekly_Sales~.,random=~1|Store/Dept,data=train, correlation=corAR1())
summary(MLMStoreDept)
MLMType<-lme(Weekly_Sales~.,random=~1|Type,data=train, correlation=corAR1())
summary(MLMStoreDept)
train$Store<-factor(train$Store)
train$Dept<-factor(train$Dept)
train$Date<-factor(train$Date)
MLMStoreDept1<-lme(Weekly_Sales~.,random=~1|Store/Dept,data=train, correlation=corAR1())
#Predictions: 
train$predMLM<-predict(MLMStoreDept,data=train)
test$predMLM<-predict(MLMStoreDept,test)
train_RMSE_MLM<-getRMSE(train$predMLM,train$Weekly_Sales)
train_Rsquare_MLM<-getR2(train$predMLM,train$Weekly_Sales)
b<-which(is.na(test$predMLM)==TRUE)

##2nd MLM 
MLMTypeStoreDept<-lme(Weekly_Sales~.,random=~1|Type/Store/Dept,data=train, correlation=corAR1())

##Predictions
train$predMLM_TSD<-predict(MLMTypeStoreDept,data=train)
test$predMLM_TSD<-predict(MLMTypeStoreDept,test)

train_RMSE_MLM_TSD<-getRMSE(train$predMLM_TSD,train$Weekly_Sales)
train_Rsquare_MLM_TSD<-getR2(train$predMLM_TSD,train$Weekly_Sales)

b<-which(is.na(test$predMLM_TSD)==TRUE)

for (i in 1:nrow(test)){
if(is.na(test$predMLM_TSD[i])==TRUE) {
test$predMLM_TSD[i]<-test$predLM[i]
}
}
test_RMSE_MLM_TSD<-getRMSE(test$predMLM_TSD,test$Weekly_Sales)
test_Rsquare_MLM_TSD<-getR2(test$predMLM_TSD,test$Weekly_Sales


Plots for MLM:
library(ggplot2)

p <- ggplot(aes(x=actual, y=pred),
            data=data.frame(actual=test$Weekly_Sales,pred=test$predMLM))

p + geom_point() +
  geom_abline(color="blue") +
  ggtitle(paste("MLM Regression in Test r^2=0.8866" )) 
  
  rm(list=ls())
X1 <- read.csv("C:/Users/Bellamkonda/Documents/walmart.csv")

indexes<- sample(1:nrow(X1),size=0.3*nrow(X1))
train1<-X1[indexes,]
indexes<- sample(1:nrow(train1),size=0.3*nrow(train1))
train<-train1[-indexes,]
test<-train1[indexes,]

b<-which(is.na(test == TRUE))

library(randomForest)
rf.model <- randomForest(Weekly_Sales~., data=train, importance=T, ntree=10)
predictedtrain.rf <- predict(rf.model,train)
train$predit <- predictedtrain.rf
train <- data.frame(train)

rf.model1 <- randomForest(Weekly_Sales~., data=test, importance=T, ntree=10)
predictedtest.rf <- predict(rf.model,test)
test$predit <- predictedtest.rf
test <- data.frame(test)
install.packages("ggplot2")
library(ggplot2)

p <- ggplot(aes(x=actual, y=pred),
            data=data.frame(actual=train$Weekly_Sales,pred=predict(rf.model,train)))

p + geom_point() +
  geom_abline(color="red") +
  ggtitle(paste("RandomForest Regression in Train r^2=0.9471" )) 


  
  






