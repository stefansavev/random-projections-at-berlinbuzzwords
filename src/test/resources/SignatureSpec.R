# a file to test the signature implementation

file <- "D:/tmp/cos-overlap.txt"
#read a file containing (cosine, signature_overlap)
table <- read.table(file, header = FALSE, sep = "\t")
cosine <- table[,1]
overlap <- table[,2] #random proj. overlap
est_cosine_file <- table[,3]
num_sig = 64*64
p_agree = overlap/num_sig

#Source for the following formula:
# Similarity Estimation Techniques from Rounding Algorithms Moses S. Charikar
#p_agree = 1.0 - angle/pi
#solve for angle
angle = (1.0 - p_agree)*pi
est_cosine = cos(angle)
plot(cosine, est_cosine)
hist(cosine - est_cosine)
sd(cosine)
sd(est_cosine)
error = sum(abs(cosine - est_cosine))/length(cosine)

#other
plot(cosine, overlap)
sd(cosine)
sd(overlap)
model = lm(cosine ~ overlap)
est_cosine_model <- predict(model)
hist(cosine - est_cosine_model )
plot(cosine, est_cosine_model)
plot(est_cosine, est_cosine_model)
plot(cosine, est_cosine_file)

sd(est_cosine_model)

plot(est_cosine_file, cosine)
plot(est_cosine_file, est_cosine)

cosine_to_overlap <- function(cos_value){
	angle = acos(cos_value)
	overlap = 1.0 - angle/pi
	return (overlap)
}

