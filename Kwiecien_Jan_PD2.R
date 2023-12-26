library("dplyr") 
library("sqldf")
library("data.table")
library("microbenchmark")

options(stringsAsFactors=FALSE)

Badges <- read.csv("travel_stackexchange_com/Badges.csv")
head(Badges)

Comments <- read.csv("travel_stackexchange_com/Comments.csv")
head(Comments)

PostLinks <- read.csv("travel_stackexchange_com/PostLinks.csv")
head(PostLinks)

Posts <- read.csv("travel_stackexchange_com/Posts.csv")
head(Posts)

Tags <- read.csv("travel_stackexchange_com/Tags.csv")
head(Tags)

Users <- read.csv("travel_stackexchange_com/Users.csv")
head(Users)

Votes <- read.csv("travel_stackexchange_com/Votes.csv")
head(Votes)


# Zad.1


# a) sql

df_sql_1 <- function(Tags){

  sqldf("SELECT Count, TagName
      FROM Tags
      WHERE Count > 1000
      ORDER BY Count DESC")
}

df_sql_1(Tags)


# b) funkcje bazowe

df_base_1 <- function(Tags){
  
  x <- Tags[ , c("Count", "TagName") ] 
  x <- x[x$Count > 1000,]
  x <- x[order(x$Count, decreasing = TRUE),]
  rownames(x) <- NULL
  wynik <- x
  
  return (wynik)

} 

df_base_1(Tags)


# c) dplyr

df_dplyr_1 <- function(Tags){
  
  Tags %>%
  select(Count, TagName) %>%
  arrange(desc(Count)) %>%
  filter(Count > 1000) -> wynik
  
  return (wynik)
  
}

df_dplyr_1(Tags)


# d) data.table

df_table_1 <- function(Tags){
  
  dt <- as.data.table(Tags)
  dt <- dt[Count > 1000][order(-Count)]
  wynik <- dt[, c("Count", "TagName")]
  
  return (wynik)
    
}

df_table_1(Tags)

## Sprawdzenie wyników

all_equal(df_sql_1(Tags), df_base_1(Tags))
all_equal(df_sql_1(Tags), df_dplyr_1(Tags))
all_equal(df_sql_1(Tags), df_table_1(Tags))


# Zad.2 


# a) sql

df_sql_2 <- function(Users, Posts){
  
  sqldf("SELECT Location, COUNT(*) AS Count
          FROM (
              SELECT Posts.OwnerUserId, Users.Id, Users.Location
              FROM Users
              JOIN Posts ON Users.Id = Posts.OwnerUserId
              )
              WHERE Location NOT IN ('')
              GROUP BY Location
              ORDER BY Count DESC
              LIMIT 10")
}

df_sql_2(Users, Posts)


# b) funkcje bazowe

df_base_2 <- function(Users, Posts){

  x <- merge(Users, Posts, by.x = "Id", by.y = "OwnerUserId", all.x = FALSE, all.y = FALSE) 
  x <- x[x$Location != '', c("Id", "Location")]
  x <- aggregate(x = x$Location, by = x["Location"], FUN = length)
  colnames(x) <- c("Location", "Count")
  x <- x[order(x$Count, decreasing = TRUE),]
  x <- head(x, 10)
  rownames(x) <- NULL
  wynik <- x
  
  return (wynik)
  
} 

df_base_2(Users, Posts)


# c) dplyr

df_dplyr_2 <- function(Users, Posts){
  
  
    select(Users, c(Id, Location)) -> pom
    select(Posts, OwnerUserId) -> oui
    inner_join(pom, oui, by = c("Id" = "OwnerUserId")) %>%
    dplyr::select(Location) %>%
    filter(Location != '') %>%
    group_by(Location) %>%
    summarise(Count = n()) -> wynik 
     
    arrange(wynik, desc(Count)) %>%
    slice_head(n = 10) -> wynik
    wynik <- as.data.frame(wynik)
    
    return (wynik)

}

df_dplyr_2(Users, Posts) 

# d) data.table

df_table_2 <- function(Users, Posts){
  
  users <- as.data.table(Users)
  posts <- as.data.table(Posts)
  
  pomposts <- posts[, "OwnerUserId"]
  pomusers <- users[, c("Id", "Location")]
  
  setkey(pomposts, OwnerUserId)
  setkey(pomusers, Id)
  
  pom <- pomposts[pomusers, nomatch=0]
  
  wynik <- pom[, "Location"]
  wynik <- as.data.table(wynik)
  wynik <- wynik[Location != '']
  wynik <- wynik[, .N, by = Location]
  wynik <- wynik[order(-N)]
  setnames(wynik, "N", "Count")
  wynik <- head(wynik, 10)
  
  return (wynik)


}


df_table_2(Users,Posts)


## Sprawdzenie wyników

all_equal(df_sql_2(Users, Posts), df_base_2(Users, Posts))
all_equal(df_sql_2(Users, Posts), df_dplyr_2(Users, Posts))
all_equal(df_sql_2(Users, Posts), df_table_2(Users,Posts))


# Zad. 3


# a) sql

df_sql_3 <- function(Badges){
  
  sqldf("SELECT Year, SUM(Number) AS TotalNumber
        FROM (
          SELECT
          Name,
          COUNT(*) AS Number,
          STRFTIME('%Y', Badges.Date) AS Year
          FROM Badges
          WHERE Class = 1
          GROUP BY Name, Year
        )
        GROUP BY Year
        ORDER BY TotalNumber")
}

df_sql_3(Badges)


# b) funkcje bazowe

df_base_3 <- function(Badges){
  
  x <- Badges[Badges$Class == 1, c("Name","Date")]
  x$Date <- strftime(x$Date, format = '%Y')
  x <- aggregate(x = x$Date, by = x["Date"], FUN = length)
  colnames(x) <- c("Year", "TotalNumber")
  x <- x[order(x$TotalNumber, decreasing = FALSE),]
  rownames(x) <- NULL
  wynik <- x
  
  return (wynik)
} 

df_base_3(Badges)


# c) dplyr

df_dplyr_3 <- function(Badges){
  
  Badges %>%
    filter(Class == 1) %>%
    mutate(Date = strftime(Date, format = "%Y")) %>%
    rename(Year = Date) %>%
    select(Name, Year) %>%
    group_by(Year) %>%
    summarise(TotalNumber = n()) %>%
    select(Year, TotalNumber) %>%
    arrange(TotalNumber) -> wynik
  
  wynik <- as.data.frame(wynik)
    
  
  return (wynik)

}


df_dplyr_3(Badges)


# d) data.table

df_table_3 <- function(Badges){
  
  badges <- as.data.table(Badges)
  badges <- badges[Class == 1]
  badges <- badges[, c("Name", "Date")]
  badges <- badges[,Year := strftime(Date, format = '%Y')]  
  badges <- badges[, .N, by = Year]
  badges <- badges[order(N)]
  setnames(badges, "N", "TotalNumber")
  wynik <- badges


  return (wynik)
  
}

df_table_3(Badges)


## Sprawdzenie wyników

all_equal(df_sql_3(Badges), df_base_3(Badges))
all_equal(df_sql_3(Badges), df_dplyr_3(Badges))
all_equal(df_sql_3(Badges), df_table_3(Badges))



# Zad. 4


# a) sql

df_sql_4 <- function(Users, Posts){
  
  sqldf("SELECT
        Users.AccountId,
        Users.DisplayName,
        Users.Location,
        AVG(PostAuth.AnswersCount) as AverageAnswersCount
        FROM
        (
          SELECT
          AnsCount.AnswersCount,
          Posts.Id,
          Posts.OwnerUserId
          FROM (
            SELECT Posts.ParentId, COUNT(*) AS AnswersCount
            FROM Posts
            WHERE Posts.PostTypeId = 2
            GROUP BY Posts.ParentId
          ) AS AnsCount
          JOIN Posts ON Posts.Id = AnsCount.ParentId
        ) AS PostAuth
        JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
        GROUP BY OwnerUserId
        ORDER BY AverageAnswersCount DESC, AccountId ASC
        LIMIT 10")
}

df_sql_4(Users, Posts)


# b) funkcje bazowe

df_base_4 <- function(Users, Posts){
  
  AnsCount <- Posts[Posts$PostTypeId == 2,]
  AnsCount <- aggregate(x = AnsCount$ParentId, by = AnsCount["ParentId"], FUN = 'length')
  colnames(AnsCount) <- c("ParentId", "AnswersCount")
  
  
  PostAuth <- merge(Posts, AnsCount, by.x = "Id", by.y = "ParentId")
  PostAuth <- PostAuth[, c("AnswersCount", "Id", "OwnerUserId")]
  
  wynik <- merge(x = Users,y = PostAuth, by.x = "AccountId", by.y = "OwnerUserId")
  wynik <- wynik[,c("AccountId", "DisplayName", "Location", "AnswersCount")]
  
  avg <- aggregate(wynik$AnswersCount, by = wynik["AccountId"], FUN = mean)
  
  wynik <- wynik[, c("AccountId", "DisplayName", "Location")]
  wynik <- merge(x = avg,y = wynik, by.x = "AccountId", by.y = "AccountId")
  colnames(wynik) <- c("AccountId",  "AverageAnswersCount", "DisplayName", "Location")
  wynik <- wynik[, c("AccountId", "DisplayName", "Location", "AverageAnswersCount")]
  wynik <- wynik[order(wynik$AverageAnswersCount, wynik$AccountId, decreasing = c(TRUE, FALSE)),]
  wynik <- head(wynik, 10)
  rownames(wynik) <- NULL
  
  
  return (wynik)

} 

df_base_4(Users, Posts)


# c) dplyr

df_dplyr_4 <- function(Users, Posts){

  Posts %>%
    filter(PostTypeId == 2) %>%
    select(ParentId) %>%
    group_by(ParentId) %>%
    summarise(AnswersCount = n()) -> AnsCount
  
  inner_join(Posts, AnsCount, by = c("Id" = "ParentId")) %>% 
    select (AnswersCount, Id, OwnerUserId) -> PostAuth
  

  inner_join(Users, PostAuth, by = c("AccountId" = "OwnerUserId"))  -> wynik
  wynik %>%
    select(AccountId, DisplayName, Location, AnswersCount) -> wynik
  
  wynik %>%
    group_by(AccountId) %>%
    summarise(AverageAnswersCount = mean(AnswersCount)) -> avg
  
  wynik %>%
    select(AccountId, DisplayName, Location) -> wynik
  
  inner_join(avg, wynik, by = "AccountId") %>%
    select(AccountId, DisplayName, Location, AverageAnswersCount) %>%
    arrange(desc(AverageAnswersCount), AccountId) %>%
    slice_head(n = 10) -> wynik
  
  wynik <- as.data.frame(wynik)
    
  return (wynik)
  
}


df_dplyr_4(Users, Posts)


# d) data.table

df_table_4 <- function(Users, Posts){
    
    posts <- as.data.table(Posts)
    users <- as.data.table(Users)
    
    AnsCount <- posts[PostTypeId == 2, "ParentId"]
    AnsCount <- AnsCount[, .N, by = ParentId]
    setnames(AnsCount, "N", "AnswersCount")
    
    setkey(posts, Id)
    setkey(AnsCount, ParentId)
    
    PostAuth <- posts[AnsCount, nomatch = 0]
    PostAuth  <- PostAuth[, c("AnswersCount", "Id", "OwnerUserId")]
    
    setkey(users, AccountId)
    setkey(PostAuth, OwnerUserId)
    
    wynik <- users[PostAuth, nomatch = 0]
    wynik <- wynik[,c("AccountId", "DisplayName", "Location", "AnswersCount")]
    
    avg <- wynik[, .(AverageAnswersCount = mean(AnswersCount)), by = AccountId]
    
    wynik <- wynik[, c("AccountId", "DisplayName", "Location")]

    setkey(wynik, AccountId)
    setkey(avg, AccountId)

    wynik <- wynik[avg, nomatch = 0]
    
    wynik <- wynik[c(order(-AverageAnswersCount), order(AccountId))]
    wynik <- head(wynik, 10)
    
  return (wynik)

}

df_table_4(Users, Posts)


## Sprawdzenie wyników

all_equal(df_sql_4(Users, Posts), df_base_4(Users, Posts))
all_equal(df_sql_4(Users, Posts), df_dplyr_4(Users, Posts))
all_equal(df_sql_4(Users, Posts), df_table_4(Users, Posts))



  
# Zad. 5


# a) sql

df_sql_5 <- function(Posts, Votes){
  
  sqldf("SELECT Posts.Title, Posts.Id,
        STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
        VotesByAge.Votes
        FROM Posts
        JOIN (
          SELECT
          PostId,
          MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
          MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
          SUM(Total) AS Votes
          FROM (
            SELECT
            PostId,
            CASE STRFTIME('%Y', CreationDate)
            WHEN '2021' THEN 'new'
            WHEN '2020' THEN 'new'
            ELSE 'old'
            END VoteDate,
            COUNT(*) AS Total
            FROM Votes
            WHERE VoteTypeId IN (1, 2, 5)
            GROUP BY PostId, VoteDate
          ) AS VotesDates
          GROUP BY VotesDates.PostId
          HAVING NewVotes > OldVotes
        ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
        WHERE Title NOT IN ('')
        ORDER BY Votes DESC
        LIMIT 10")
}

df_sql_5(Posts, Votes)


# b) funkcje bazowe

 df_base_5 <- function(Posts, Votes){


  VotesDates <- Votes[c(Votes$VoteTypeId == 1 | Votes$VoteTypeId == 2 | Votes$VoteTypeId == 5),]
  VotesDates$CreationDate <- strftime(VotesDates$CreationDate, '%Y')
  VotesDates <- VotesDates[, c("PostId", "CreationDate")]
  VotesDates["CreationDate"][VotesDates["CreationDate"] == "2021"] <- "new"
  VotesDates["CreationDate"][VotesDates["CreationDate"] == "2020"] <- "new"
  VotesDates["CreationDate"][VotesDates["CreationDate"] != "new"] <- "old"
  VotesDates <- aggregate(x = VotesDates$CreationDate, by = VotesDates[c("PostId",  "CreationDate")], FUN = 'length')
  VotesDates <- VotesDates[order(VotesDates$PostId, decreasing = FALSE), ]
  rownames(VotesDates) <- NULL
  colnames(VotesDates) <- c("PostId", "VoteDate", "Total")
  

  VotesByAge1 <- VotesDates[VotesDates$VoteDate == "new", ]
  VotesByAge2 <- VotesDates[VotesDates$VoteDate == "old", ]
  pomdf <- data.frame (PostId  = seq(1, 151674, by = 1), VoteDate = "old", Total = 0)
  
  VotesByAge2 <- merge(x = VotesByAge2, y = pomdf, by.x = "PostId", by.y = "PostId",  all.y = TRUE)
  VotesByAge2 <- subset(VotesByAge2, select = -c(VoteDate.x))
  VotesByAge2[is.na(VotesByAge2)] <- 0
  
  VotesByAge2$Total.x + VotesByAge2$Total.y -> Total
  VotesByAge2 <- subset(VotesByAge2, select = -c(Total.x, Total.y))
  Total <- as.data.frame(Total)
  VotesByAge2 <- cbind(VotesByAge2, Total)
  
  VotesByAge <- merge(x = VotesByAge1, y = VotesByAge2, by.x = "PostId", by.y= "PostId", all.x = TRUE, all.y = TRUE)
  VotesByAge <- subset(VotesByAge, select = -c(VoteDate, VoteDate.y))
  VotesByAge[is.na(VotesByAge)] <- 0
  colnames(VotesByAge) <- c("PostId", "NewVotes", "OldVotes")
  VotesByAge <- subset(VotesByAge, VotesByAge$NewVotes > VotesByAge$OldVotes)
  
  votes <- VotesByAge$NewVotes + VotesByAge$OldVotes
  VotesByAge <- cbind(VotesByAge, votes)
  colnames(VotesByAge) <- c("PostId", "NewVotes", "OldVotes", "Votes")
  rownames(VotesByAge) <- NULL
  
  
  wynik <- merge(x = Posts, y = VotesByAge, by.x = "Id", by.y = "PostId")
  wynik <- wynik[wynik$Title != '', c("Title", "Id", "CreationDate", "Votes")]
  wynik$CreationDate <- strftime(wynik$CreationDate, '%Y-%m-%d')
  colnames(wynik) <- c("Title", "Id", "Date", "Votes")
  wynik <- wynik[order(wynik$Votes, decreasing = TRUE),]
  wynik <- head(wynik, 10)
  wynik$Votes <- as.integer(wynik$Votes)
  rownames(wynik) <- NULL

  
  return (wynik)

}

df_base_5(Posts, Votes)


# c) dplyr

df_dplyr_5 <- function(Posts, Votes){
  
  Votes %>%
    filter(VoteTypeId == 1 | VoteTypeId == 2 | VoteTypeId == 5) %>%
    mutate(VoteDate = strftime(CreationDate, format = "%Y")) %>%
    select(PostId, VoteDate) -> VotesDates
  
  VotesDates %>%
    mutate(VoteDate = replace(VoteDate, VoteDate == "2021", "new")) %>%
    mutate(VoteDate = replace(VoteDate, VoteDate == "2020", "new")) %>%
    mutate(VoteDate = replace(VoteDate, VoteDate != "new", "old")) -> VotesDates
  
  VotesDates %>%
    group_by(PostId, VoteDate) %>% 
    summarise(Total = n()) -> VotesDates
  
  VotesDates %>%
    filter(VoteDate == "new") %>%
    select(PostId, VoteDate, Total) -> VotesByAge1
  
  VotesDates %>%
    filter(VoteDate == "old") %>%
    select(PostId, VoteDate, Total) -> VotesByAge2 
  
  pomdf <- data.frame(PostId  = seq(1, 151674, by = 1), VoteDate = "old", Total = 0)
  
  full_join(pomdf, VotesByAge2, by = "PostId") -> VotesByAge2
  
  VotesByAge2 %>%
    select(PostId, VoteDate.x, Total.x, Total.y) %>%
    replace(is.na(.), 0) -> VotesByAge2
  
  VotesByAge2 %>%
    rowwise() %>%
    mutate(sum(Total.x, Total.y)) -> VotesByAge2
  
  VotesByAge2 %>%
    as.data.frame() %>%
    select(PostId, VoteDate.x, Total.y) %>%
    rename(VoteDate = VoteDate.x) %>%
    rename(Total = Total.y) -> VotesByAge2
  
  full_join(VotesByAge2, VotesByAge1, by = "PostId") %>%
    select(PostId, Total.x, Total.y) %>%
    replace(is.na(.), 0) %>%
    relocate(Total.y, .after=PostId) %>%
    rename(NewVotes = Total.y) %>%
    rename(OldVotes = Total.x) %>%
    filter(NewVotes > OldVotes) -> VotesByAge
  
  VotesByAge %>%
    rowwise() %>%
    mutate(Votes = sum(NewVotes, OldVotes)) %>%
    as.data.frame() -> VotesByAge
    
  inner_join(Posts, VotesByAge, by = c("Id" = "PostId")) %>%
    filter(Title != '') %>%
    select(Title, Id, CreationDate, Votes) %>%
    mutate(Date = strftime(CreationDate, format = "%Y-%m-%d")) %>%
    select(Title, Id, Votes, Date) %>%
    relocate(Date, .after = Id) %>%
    arrange(desc(Votes)) %>%
    slice_head(n = 10) %>%
    mutate(across(where(is.double), as.integer)) -> wynik
    
    
  return (wynik)
  
}


df_dplyr_5(Posts, Votes)

# d) data.table

df_table_5 <- function(Posts, Votes){
  
  posts <- as.data.table(Posts)
  votes <- as.data.table(Votes)
  
  VotesDates <- votes[VoteTypeId == 1 | VoteTypeId == 2 | VoteTypeId == 5,]
  VotesDates <- VotesDates[,CreationDate := strftime(CreationDate, format = '%Y')]
  VotesDates <- VotesDates[, c("PostId", "CreationDate")]
  VotesDates[CreationDate == "2021","CreationDate"] <- "new"
  VotesDates[CreationDate == "2020","CreationDate"] <- "new"
  VotesDates[CreationDate != "new","CreationDate"] <- "old"
  VotesDates <- VotesDates[, .N, by = list(PostId,  CreationDate) ]
  VotesDates <- VotesDates[order(PostId)]
  setnames(VotesDates, "N", "Total")
  setnames(VotesDates, "CreationDate", "VoteDate")
  
  VotesByAge1 <- VotesDates[VoteDate == "new"]
  VotesByAge2 <- VotesDates[VoteDate == "old"]
  pomdt <-data.table(PostId = seq(1, 151674, by = 1), VoteDate = "old", Total = 0)
  
  setkey(VotesByAge2, PostId)
  setkey(pomdt, PostId)
  
  VotesByAge2 <- VotesByAge2[pomdt, nomatch = 0]
  VotesByAge2 <- VotesByAge2[, !"i.VoteDate"]
  VotesByAge2 <- VotesByAge2[, !"i.Total"]

  VotesByAge <- merge.data.table(VotesByAge1, VotesByAge2, by = "PostId", all = TRUE)
  VotesByAge[is.na(VotesByAge)] <- 0
  VotesByAge <- VotesByAge[, !"VoteDate.x"]
  VotesByAge <- VotesByAge[, !"VoteDate.y"]
  setnames(VotesByAge, "Total.x", "NewVotes")
  setnames(VotesByAge, "Total.y", "OldVotes")
  VotesByAge <- VotesByAge[NewVotes > OldVotes,]
  
  votespom <- VotesByAge[,NewVotes] + VotesByAge[,OldVotes]
  votespom <- as.data.table(votespom)
  VotesByAge <- cbind(VotesByAge, votespom)
  setnames(VotesByAge, "votespom", "Votes")
  
  wynik <- merge.data.table(x = Posts, y = VotesByAge, by.x = "Id", by.y = "PostId")
  wynik <- as.data.table(wynik)
  wynik <- wynik[Title != '',]
  wynik <- wynik[,c("Title", "Id", "CreationDate", "Votes")]
  wynik <- wynik[,CreationDate := strftime(CreationDate, format = '%Y-%m-%d')]
  setnames(wynik, "CreationDate",  "Date")
  wynik <- wynik[order(-Votes)]
  wynik <- head(wynik, 10)
  wynik <- wynik[, Votes:=as.integer(Votes)]
  wynik <- as.data.frame(wynik)

  
  return (wynik)
  
}


df_table_5(Posts, Votes)


## Sprawdzenie wyników

all_equal(df_sql_5(Posts, Votes), df_base_5(Posts, Votes))
all_equal(df_sql_5(Posts, Votes), df_dplyr_5(Posts, Votes))
all_equal(df_sql_5(Posts, Votes), df_table_5(Posts, Votes))




