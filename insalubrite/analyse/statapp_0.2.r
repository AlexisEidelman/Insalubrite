library(foreign)
library(questionr)
library(ggplot2)
library(scales)
library(stargazer)
library(mfx)
library(FactoMineR)
library(pscl)
library(MASS)
library(VGAM)
library(lasso2)
library(glmnet)
library(leaps)
library(AER)
library(lmtest)
library(xtable)
library(sampleSelection)

data = read.csv("D:/data/SARAH/output/base.csv")
data_parcelle = read.csv("D:/data/SARAH/output/niveau_parcelles.csv")
data_adresse = read.csv("D:/data/SARAH/output/niveau_adresses.csv")

data$indic <- duplicated(data$affaire_id)
data <- subset(data, data$indic == "FALSE")

data_adresse$indic <- duplicated(data_adresse$affaire_id)
data_adresse <- subset(data_adresse, data_adresse$indic == "FALSE")

data_parcelle$indic <- duplicated(data_parcelle$affaire_id)
data_parcelle <- subset(data_parcelle, data$indic == "FALSE")

data <- merge(data,data_adresse,by="affaire_id",all.x = TRUE)
data <- unique(data)

data <- merge.data.frame(data, data_parcelle, by="affaire_id",all.x = TRUE)
data$indic <- duplicated(data$affaire_id)
data <- subset(data, data$indic == "FALSE")

data <- data[!(names(data) %in% c("affaire_id","adresse_ban","adresse_ban_id.x","adresse_ban_score","adresse_ban_type","adresse_id" ))] 
data <- data[!(names(data) %in% c("batiment_id","bien_id","bien_id_provenance","code_cadastre","codeinsee"))] 
data <- data[!(names(data) %in% c("daterecolement","designation","hauteur_facade","immeuble_id"))] 
data <- data[!(names(data) %in% c("islogement","libelle","localhabite_id","nomcommune","noterecolement","numero_ilot"))] 
data <- data[!(names(data) %in% c("numlot","observations_batiment","observations_immeuble","observations_localhabite","parcelle_id"))] 
data <- data[!(names(data) %in% c("type_bien_concerne","typeadresse","indic.x","adresse_ban_id.y","infractiontype_id.y","titre.y"))] 
data <- data[!(names(data) %in% c("infractiontype_id.x","titre.x","eau_annee_source","sat_annee_source","indic.y"))] 
data <- data[!(names(data) %in% c("N_SQ_PC","N_SQ_PD","C_PDNIV0","L_PDNIV0","C_PDNIV1","C_PDNIV2"))] 
data <- data[!(names(data) %in% c("X...","Logé.chez.d.autres.personnes","Centre.départemental.de.l.enfance.et.de.la.famille.ou.centre.maternel"))]                          
data <- data[!(names(data) %in% c("Foyers.agents.Ville.ou.agents.CASVP","Logé.dans.un.logement.de.fonction.par.votre.employeur"))]   
data <- data[!(names(data) %in% c("Logé.dans.un.foyer","Logé.à.titre.gratuit","Logé.dans.un.hôtel.social..par.un.centre.d.hébergement..un.logement.d.urgence.ou.une.association"))]
data <- data[!(names(data) %in% c("Logé.à.l.hôtel","Dans.un.local.non.destiné.à.l.habitat..cave..parking..etc..","Logé.chez.des.parents"))]
data <- data[!(names(data) %in% c("Propriétaire","Locataire.dans.le.privé","Résidence.étudiant","Résidence.hôtelière.à.vocation.sociale"))]                                                      
data <- data[!(names(data) %in% c("Sans.domicile.fixe","Structure.d.hébergement","Sous.locataire.ou.hébergé.dans.un.logement.à.titre.temporaire"))]
data <- data[!(names(data) %in% c("Locataire.dans.un.logement.social","Louez.solidaire.et.sans.risque","Dans.un.squat","TOT_PROP"))]                                                                                        
data <- data[!(names(data) %in% c("TOT_LOC","TOTAL_DEM","indic"))] 
data <- data[!(names(data) %in% c("B_GRAPH","OBJECTID","ASP","X...","Logé.chez.d.autres.personnes","Centre.départemental.de.l.enfance.et.de.la.famille.ou.centre.maternel"))]

#Création de la variable indicatrice insalubre
# levels(data$possedecaves) <- c("0","0","1") # data$possedecaves[is.na(data$possedecaves)] <- 2
# levels(data$copropriete) <- factor(data$copropriete)
# levels(data$copropriete) <- c("0","1")
# levels(data$parties_cummunes) <- c("0","1")
# levels(data$possedecaves) <- c("0","0","1")

data$insalubre <- sapply(!is.na(data$infractiontype_id), function(x){as.integer(x)})
data$gardien <- sapply(data$mode_entree_batiment =="Gardien", function(x){as.integer(x)})

data$risque <- data$infractiontype_id

for(k in 1:n){
  if(data$articles[k]!=""){
    data$risque[k] <- 2
  }
  
  if(data$articles[k]==""){
    data$risque[k] <- 1
  }
  
  if(data$insalubre[k]==0){
    data$risque[k] <- 0
  }
  
}

data$risque <- factor(data$risque)
data$codepostal<- factor(data$codepostal)
data$codepostal <- relevel(data$codepostal, ref="75018")
data$possedecaves <- factor(data$possedecaves)
data$Assechement.de.locaux <- factor(data$Assechement.de.locaux)
data$Chaudiere.surchauffee <- factor(data$Chaudiere.surchauffee)
data$Court.circuit <- factor(data$Court.circuit)
data$Debouchage.d.egout.ou.de.canalisation <- factor(data$Debouchage.d.egout.ou.de.canalisation)
data$Eboulement..effondrement <- factor(data$Eboulement..effondrement)
data$Feu.appel.douteux <- factor(data$Feu.appel.douteux)
data$Feu.ayant.existe <- factor(data$Feu.ayant.existe)
data$Feu.de....<-factor(data$Feu.de....)
data$Feu.de.cheminee <- factor(data$Feu.de....)
data$Fuite.d.eau <- factor(data$Fuite.d.eau)

#Stat descriptive

levels(data$titre) <- c("","éclairage","éclairage","équipement","éclairage","décence","eau","eau","électricité","eau","eau","fenêtres","couverture","humidité","humidité","infiltration","infiltration","animaux","éclairage","","localisation","localisation","sale","sale","animaux","eau","équipement","animaux","sols/murs","sols/murs","sale","surface","équipement")
levels(data$titre)
plot(data$titre)
summary(data$articles)

summary(data$codepostal)
plot(data$codepostal, col="blue")

#Focus quartier
data1 <- subset(data, data$insalubre ==1)
data1 <- subset(data1,data1$codepostal=="75010")
data1$quartier_admin <- factor(data1$quartier_admin)
data2 <- subset(data, data$insalubre ==0)
plot(data1$codepostal, col="red")
plot(data2$codepostal, col="green")
plot(data1$quartier_admin)

tab <- table(data$codepostal,data$niveau)
mosaicplot(tab)
stargazer(tab)
chisq.test(data$quartier_admin,data$niveau)
chisq.test(data$codepostal,data$niveau)

fit <- aov(risque ~ codepostal, data=data) # y est la variable numérique et A indique les groupes
summary(fit)

#ACM
data_ACM <- data[c("titre","articles","B_PUBLIC","copropriete","codepostal","parties_cummunes","possedecaves","gardien","numetage","L_PDNIV1","Chaudiere.surchauffee","NB_NIVBAT","NB_LOCACT","NB_LG_VAC")]
names(data_ACM) <- c("titre","articles","public","copropriete","codepostal","parties communes","caves","gardien","étage","propriétaire","chaudière","niveau batiment","locataire","vacant")
ACM = function(data,n){
  res = MCA(data, ncp = n, graph = TRUE)
  barplot(res$eig[, 2], main= "Histogramme des valeurs propres", names.arg=rownames(res$eig), xlab= "Axes", ylab= "Pourcentage d’inertie", cex.axis=0.8, font.lab=3, col= "orange")
  #plot.MCA(res, choix = "var")
  summary(res)
}

data_ACM <- as.data.frame(lapply(data_ACM, as.factor))

ACM(data_ACM,2)

#Binaire catégorielle
levels(data$B_PUBLIC) <- c(0,0,1)
datareg <- data[c("risque","insalubre","copropriete","codepostal","parties_cummunes","possedecaves","gardien","B_PUBLIC","Chaudiere.surchauffee","Prelevement.monoxyde.de.carbone")]
datareg$codepostal <- factor(datareg$codepostal)
datareg$codepostal <- relevel(datareg$codepostal, ref="18")

logit = glm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, family=binomial(link=logit),datareg)
probit = glm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, family=binomial(link=probit),datareg)
summary(logit)
summary(probit)
pR2(logit)
pR2(probit)
logitmfx(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, datareg, robust=TRUE)
stargazer(logit)

#IV
datareg<- data[c("insalubre","NB_LOCACT","AN_MIN","NB_PIEC_2","copropriete","codepostal","parties_cummunes","possedecaves","gardien","B_PUBLIC","Panne.d.origine.electrique","Fuite.d.eau","risque")]
datareg$codepostal <- factor(datareg$codepostal)
datareg$codepostal <- relevel(datareg$codepostal, ref="75018")
datareg$gardien <- factor(datareg$gardien)
datareg <- na.omit(datareg)
first <- lm(Fuite.d.eau ~ Panne.d.origine.electrique + NB_PIEC_2 + copropriete + codepostal + parties_cummunes + possedecaves + gardien + AN_MIN + NB_LOCACT,data=datareg)
summary(first)
datareg$estimate <- fitted.values(first)
second <- lm(insalubre ~ estimate + copropriete + codepostal + parties_cummunes + possedecaves + gardien +AN_MIN + NB_LOCACT + NB_PIEC_2,data=datareg)
summary(second)

#Polytomique

logit=vglm(risque~copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC,family = multinomial ,data=datareg)
summary(logit) 
xtable(coef(summary(logit)))

#Reg

levels(data$codepostal) <- c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
data$codepostal <- as.numeric(data$codepostal)
data$possedecaves <- as.numeric(data$possedecaves)
levels(data$parties_cummunes) <- c(0,1)
data$parties_cummunes <- as.numeric(data$parties_cummunes)

datareg <- data[c("insalubre","gardien","copropriete","codepostal","parties_cummunes","possedecaves","B_PUBLIC")]
names(data)
datareg <- na.omit(datareg)
reg1 <- lm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves, data=datareg)
summary(reg1)

#Tests
residu<-residuals(reg1)
residu2 <- residu**2
regpagan <- lm(residu2~ datareg$copropriete + datareg$codepostal + datareg$parties_cummunes + datareg$possedecaves)
summary(regpagan)

#FGLS

logresidu2 <- log(residu2)
varest <- lm(logresidu2~ datareg$copropriete + datareg$codepostal + datareg$parties_cummunes + datareg$possedecaves)
summary(varest)

datareg$sdi <- exp(-0.5*fitted.values(varest))
datareg$insalubre <- datareg$insalubre*datareg$sdi
datareg$copropriete <- datareg$copropriete*datareg$sdi
datareg$codepostal <- datareg$codepostal*datareg$sdi
datareg$parties_cummunes <- datareg$parties_cummunes*datareg$sdi
datareg$possedecaves <- datareg$possedecaves*datareg$sdi

FGLS <- lm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves, data=datareg)
summary(FGLS)
summary(reg1)

stargazer(FGLS)
stargazer(reg1)

#Binaire

datareg <- data[c("insalubre","copropriete","codepostal","parties_cummunes","possedecaves","gardien","B_PUBLIC")]

logit = glm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, family=binomial(link=logit),datareg)
probit = glm(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, family=binomial(link=probit),datareg)
summary(logit)
summary(probit)
pR2(logit)
pR2(probit)
logitmfx(insalubre ~ copropriete + codepostal + parties_cummunes + possedecaves+ gardien + B_PUBLIC, datareg, robust=TRUE)
stargazer(logit)

#Sélection
setwd("...")
rm(list=ls())

