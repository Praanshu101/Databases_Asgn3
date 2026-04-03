-- MySQL dump 10.13  Distrib 8.4.8, for Linux (x86_64)
--
-- Host: localhost    Database: shuttle_system
-- ------------------------------------------------------
-- Server version	8.4.8

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `BOOKING`
--

DROP TABLE IF EXISTS `BOOKING`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `BOOKING` (
  `BookingID` int NOT NULL AUTO_INCREMENT,
  `MemberID` int NOT NULL,
  `TripID` int NOT NULL,
  `BookedAt` datetime DEFAULT CURRENT_TIMESTAMP,
  `SeatNo` int NOT NULL,
  `Status` varchar(20) DEFAULT 'Confirmed',
  PRIMARY KEY (`BookingID`),
  KEY `MemberID` (`MemberID`),
  KEY `TripID` (`TripID`),
  CONSTRAINT `BOOKING_ibfk_1` FOREIGN KEY (`MemberID`) REFERENCES `MEMBER` (`MemberID`),
  CONSTRAINT `BOOKING_ibfk_2` FOREIGN KEY (`TripID`) REFERENCES `TRIP` (`TripID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `BOOKING`
--

LOCK TABLES `BOOKING` WRITE;
/*!40000 ALTER TABLE `BOOKING` DISABLE KEYS */;
INSERT INTO `BOOKING` VALUES (1,1,1,'2026-02-15 10:55:18',5,'Confirmed'),(2,2,1,'2026-02-15 10:55:18',6,'Confirmed'),(3,3,2,'2026-02-15 10:55:18',10,'Confirmed'),(4,4,3,'2026-02-15 10:55:18',12,'Cancelled'),(5,5,4,'2026-02-15 10:55:18',8,'Confirmed'),(6,6,5,'2026-02-15 10:55:18',15,'Confirmed'),(7,7,6,'2026-02-15 10:55:18',3,'Confirmed'),(8,8,7,'2026-02-15 10:55:18',9,'Confirmed'),(9,9,8,'2026-02-15 10:55:18',4,'Confirmed'),(10,10,9,'2026-02-15 10:55:18',7,'No-show');
/*!40000 ALTER TABLE `BOOKING` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DRIVER`
--

DROP TABLE IF EXISTS `DRIVER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `DRIVER` (
  `DriverID` int NOT NULL AUTO_INCREMENT,
  `Name` varchar(100) NOT NULL,
  `Phone` varchar(15) NOT NULL,
  `LicenseNo` varchar(50) NOT NULL,
  `ExperienceYears` int DEFAULT NULL,
  PRIMARY KEY (`DriverID`),
  UNIQUE KEY `LicenseNo` (`LicenseNo`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DRIVER`
--

LOCK TABLES `DRIVER` WRITE;
/*!40000 ALTER TABLE `DRIVER` DISABLE KEYS */;
INSERT INTO `DRIVER` VALUES (1,'Ramesh Kumar','9123456780','DL1001',5),(2,'Suresh Yadav','9234567890','DL1002',7),(3,'Mahesh Patil','9345678901','DL1003',6),(4,'Ganesh Rao','9456789012','DL1004',8),(5,'Vijay Singh','9567890123','DL1005',4),(6,'Ajay Verma','9678901234','DL1006',9),(7,'Sunil Sharma','9789012345','DL1007',5),(8,'Deepak Jha','9890123456','DL1008',6),(9,'Ravi Chauhan','9901234567','DL1009',10),(10,'Manoj Tiwari','9012345678','DL1010',3);
/*!40000 ALTER TABLE `DRIVER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MEMBER`
--

DROP TABLE IF EXISTS `MEMBER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `MEMBER` (
  `MemberID` int NOT NULL AUTO_INCREMENT,
  `Name` varchar(100) NOT NULL,
  `Age` int DEFAULT NULL,
  `Email` varchar(100) NOT NULL,
  `Phone` varchar(15) DEFAULT NULL,
  `Image` blob,
  `CreatedAt` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`MemberID`),
  UNIQUE KEY `Email` (`Email`)
  -- UNIQUE KEY `Phone` (`Phone`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MEMBER`
--

LOCK TABLES `MEMBER` WRITE;
/*!40000 ALTER TABLE `MEMBER` DISABLE KEYS */;
INSERT INTO `MEMBER` VALUES (1,'Rahul Mehta',20,'rahul@gmail.com','9876543210',NULL,'2026-02-15 10:55:18'),(2,'Ananya Shah',21,'ananya@gmail.com','9876501234',NULL,'2026-02-15 10:55:18'),(3,'Karan Patel',22,'karan@gmail.com','9876512345',NULL,'2026-02-15 10:55:18'),(4,'Sneha Joshi',19,'sneha@gmail.com','9876523456',NULL,'2026-02-15 10:55:18'),(5,'Arjun Singh',23,'arjun@gmail.com','9876534567',NULL,'2026-02-15 10:55:18'),(6,'Priya Nair',20,'priya@gmail.com','9876545678',NULL,'2026-02-15 10:55:18'),(7,'Vivek Rao',21,'vivek@gmail.com','9876556789',NULL,'2026-02-15 10:55:18'),(8,'Isha Desai',22,'isha@gmail.com','9876567890',NULL,'2026-02-15 10:55:18'),(9,'Rohan Das',24,'rohan@gmail.com','9876578901',NULL,'2026-02-15 10:55:18'),(10,'Meera Kapoor',20,'meera@gmail.com','9876589012',NULL,'2026-02-15 10:55:18');
/*!40000 ALTER TABLE `MEMBER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PENALTY`
--

DROP TABLE IF EXISTS `PENALTY`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `PENALTY` (
  `PenaltyID` int NOT NULL AUTO_INCREMENT,
  `BookingID` int NOT NULL,
  `Amount` decimal(10,2) NOT NULL,
  `Reason` varchar(255) DEFAULT NULL,
  `IssuedAt` datetime DEFAULT CURRENT_TIMESTAMP,
  `PaidStatus` varchar(20) DEFAULT 'Unpaid',
  PRIMARY KEY (`PenaltyID`),
  KEY `BookingID` (`BookingID`),
  CONSTRAINT `PENALTY_ibfk_1` FOREIGN KEY (`BookingID`) REFERENCES `BOOKING` (`BookingID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PENALTY`
--

LOCK TABLES `PENALTY` WRITE;
/*!40000 ALTER TABLE `PENALTY` DISABLE KEYS */;
INSERT INTO `PENALTY` VALUES (1,4,100.00,'Late Cancellation','2026-02-15 10:55:19','Paid'),(2,10,150.00,'No-show','2026-02-15 10:55:19','Unpaid'),(3,3,50.00,'Seat Change Fee','2026-02-15 10:55:19','Paid'),(4,2,75.00,'Delay Fee','2026-02-15 10:55:19','Paid'),(5,5,100.00,'No-show','2026-02-15 10:55:19','Unpaid'),(6,6,120.00,'Late Cancellation','2026-02-15 10:55:19','Paid'),(7,7,80.00,'No-show','2026-02-15 10:55:19','Unpaid'),(8,8,60.00,'Seat Change Fee','2026-02-15 10:55:19','Paid'),(9,9,90.00,'No-show','2026-02-15 10:55:19','Unpaid'),(10,1,40.00,'Delay Fee','2026-02-15 10:55:19','Paid');
/*!40000 ALTER TABLE `PENALTY` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROUTE`
--

DROP TABLE IF EXISTS `ROUTE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ROUTE` (
  `RouteID` int NOT NULL AUTO_INCREMENT,
  `Source` varchar(100) NOT NULL,
  `Destination` varchar(100) NOT NULL,
  `DistanceKm` decimal(5,2) DEFAULT NULL,
  `EstimatedTime` int DEFAULT NULL,
  PRIMARY KEY (`RouteID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROUTE`
--

LOCK TABLES `ROUTE` WRITE;
/*!40000 ALTER TABLE `ROUTE` DISABLE KEYS */;
INSERT INTO `ROUTE` VALUES (1,'Campus','City Center',12.00,30),(2,'Campus','Railway Station',18.00,45),(3,'Campus','Airport',25.00,60),(4,'Campus','Mall',10.00,25),(5,'Campus','Tech Park',15.00,35),(6,'Campus','Bus Stand',14.00,30),(7,'Campus','Hospital',8.00,20),(8,'Campus','Metro Station',9.00,22),(9,'Campus','Industrial Area',20.00,50),(10,'Campus','Old Town',16.00,40);
/*!40000 ALTER TABLE `ROUTE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROUTE_STOP`
--

DROP TABLE IF EXISTS `ROUTE_STOP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ROUTE_STOP` (
  `RouteID` int NOT NULL,
  `StopID` int NOT NULL,
  `StopOrder` int NOT NULL,
  PRIMARY KEY (`RouteID`,`StopID`),
  KEY `StopID` (`StopID`),
  CONSTRAINT `ROUTE_STOP_ibfk_1` FOREIGN KEY (`RouteID`) REFERENCES `ROUTE` (`RouteID`),
  CONSTRAINT `ROUTE_STOP_ibfk_2` FOREIGN KEY (`StopID`) REFERENCES `STOP` (`StopID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROUTE_STOP`
--

LOCK TABLES `ROUTE_STOP` WRITE;
/*!40000 ALTER TABLE `ROUTE_STOP` DISABLE KEYS */;
INSERT INTO `ROUTE_STOP` VALUES (1,1,1),(1,2,2),(1,4,3),(2,1,1),(2,3,2),(2,5,3),(3,1,1),(3,6,2),(4,1,1),(4,7,2);
/*!40000 ALTER TABLE `ROUTE_STOP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SCHEDULE`
--

DROP TABLE IF EXISTS `SCHEDULE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `SCHEDULE` (
  `ScheduleID` int NOT NULL AUTO_INCREMENT,
  `RouteID` int NOT NULL,
  `ShuttleID` int NOT NULL,
  `DriverID` int NOT NULL,
  `StartTime` time NOT NULL,
  `EndTime` time NOT NULL,
  `DayOfWeek` varchar(15) DEFAULT NULL,
  PRIMARY KEY (`ScheduleID`),
  KEY `RouteID` (`RouteID`),
  KEY `ShuttleID` (`ShuttleID`),
  KEY `DriverID` (`DriverID`),
  CONSTRAINT `SCHEDULE_ibfk_1` FOREIGN KEY (`RouteID`) REFERENCES `ROUTE` (`RouteID`),
  CONSTRAINT `SCHEDULE_ibfk_2` FOREIGN KEY (`ShuttleID`) REFERENCES `SHUTTLE` (`ShuttleID`),
  CONSTRAINT `SCHEDULE_ibfk_3` FOREIGN KEY (`DriverID`) REFERENCES `DRIVER` (`DriverID`),
  CONSTRAINT `check_time` CHECK ((`EndTime` > `StartTime`))
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SCHEDULE`
--

LOCK TABLES `SCHEDULE` WRITE;
/*!40000 ALTER TABLE `SCHEDULE` DISABLE KEYS */;
INSERT INTO `SCHEDULE` VALUES (1,1,1,1,'08:00:00','08:30:00','Monday'),(2,2,2,2,'09:00:00','09:45:00','Monday'),(3,3,3,3,'10:00:00','11:00:00','Tuesday'),(4,4,4,4,'11:00:00','11:30:00','Tuesday'),(5,5,5,5,'12:00:00','12:40:00','Wednesday'),(6,6,6,6,'13:00:00','13:35:00','Wednesday'),(7,7,7,7,'14:00:00','14:25:00','Thursday'),(8,8,8,8,'15:00:00','15:30:00','Thursday'),(9,9,9,9,'16:00:00','16:50:00','Friday'),(10,10,10,10,'17:00:00','17:40:00','Friday');
/*!40000 ALTER TABLE `SCHEDULE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SHUTTLE`
--

DROP TABLE IF EXISTS `SHUTTLE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `SHUTTLE` (
  `ShuttleID` int NOT NULL AUTO_INCREMENT,
  `PlateNo` varchar(20) NOT NULL,
  `Capacity` int NOT NULL,
  `Model` varchar(50) DEFAULT NULL,
  `Status` varchar(20) DEFAULT 'Active',
  PRIMARY KEY (`ShuttleID`),
  UNIQUE KEY `PlateNo` (`PlateNo`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SHUTTLE`
--

LOCK TABLES `SHUTTLE` WRITE;
/*!40000 ALTER TABLE `SHUTTLE` DISABLE KEYS */;
INSERT INTO `SHUTTLE` VALUES (1,'MH12AB1001',30,'Tata Starbus','Active'),(2,'MH12AB1002',35,'Tata Starbus','Active'),(3,'MH12AB1003',40,'Ashok Leyland','Active'),(4,'MH12AB1004',30,'Eicher Bus','Active'),(5,'MH12AB1005',32,'Tata Ultra','Active'),(6,'MH12AB1006',28,'Mini Bus','Active'),(7,'MH12AB1007',45,'Ashok Leyland','Active'),(8,'MH12AB1008',38,'Tata Starbus','Maintenance'),(9,'MH12AB1009',34,'Eicher Bus','Active'),(10,'MH12AB1010',36,'Tata Ultra','Active');
/*!40000 ALTER TABLE `SHUTTLE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `STOP`
--

DROP TABLE IF EXISTS `STOP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `STOP` (
  `StopID` int NOT NULL AUTO_INCREMENT,
  `StopName` varchar(100) NOT NULL,
  `Latitude` decimal(10,8) NOT NULL,
  `Longitude` decimal(11,8) NOT NULL,
  PRIMARY KEY (`StopID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `STOP`
--

LOCK TABLES `STOP` WRITE;
/*!40000 ALTER TABLE `STOP` DISABLE KEYS */;
INSERT INTO `STOP` VALUES (1,'Main Gate',19.07600000,72.87770000),(2,'Market',19.07700000,72.87800000),(3,'Signal Point',19.07800000,72.87900000),(4,'City Center',19.07900000,72.88000000),(5,'Railway Station',19.08000000,72.88100000),(6,'Airport',19.08100000,72.88200000),(7,'Mall',19.08200000,72.88300000),(8,'Tech Park',19.08300000,72.88400000),(9,'Hospital',19.08400000,72.88500000),(10,'Metro',19.08500000,72.88600000);
/*!40000 ALTER TABLE `STOP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TICKET`
--

DROP TABLE IF EXISTS `TICKET`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `TICKET` (
  `TicketID` int NOT NULL AUTO_INCREMENT,
  `BookingID` int NOT NULL,
  `QRCode` varchar(255) NOT NULL,
  `VerifiedAt` datetime DEFAULT NULL,
  `IsVerified` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`TicketID`),
  KEY `BookingID` (`BookingID`),
  CONSTRAINT `TICKET_ibfk_1` FOREIGN KEY (`BookingID`) REFERENCES `BOOKING` (`BookingID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TICKET`
--

LOCK TABLES `TICKET` WRITE;
/*!40000 ALTER TABLE `TICKET` DISABLE KEYS */;
INSERT INTO `TICKET` VALUES (1,1,'QR001','2026-02-15 10:55:20',1),(2,2,'QR002','2026-02-15 10:55:20',1),(3,3,'QR003','2026-02-15 10:55:20',1),(4,4,'QR004',NULL,0),(5,5,'QR005','2026-02-15 10:55:20',1),(6,6,'QR006','2026-02-15 10:55:20',1),(7,7,'QR007','2026-02-15 10:55:20',1),(8,8,'QR008','2026-02-15 10:55:20',1),(9,9,'QR009','2026-02-15 10:55:20',1),(10,10,'QR010',NULL,0);
/*!40000 ALTER TABLE `TICKET` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TRIP`
--

DROP TABLE IF EXISTS `TRIP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `TRIP` (
  `TripID` int NOT NULL AUTO_INCREMENT,
  `ScheduleID` int NOT NULL,
  `Date` date NOT NULL,
  `ActualStart` datetime DEFAULT NULL,
  `ActualEnd` datetime DEFAULT NULL,
  `Status` varchar(20) DEFAULT 'Scheduled',
  PRIMARY KEY (`TripID`),
  KEY `ScheduleID` (`ScheduleID`),
  CONSTRAINT `TRIP_ibfk_1` FOREIGN KEY (`ScheduleID`) REFERENCES `SCHEDULE` (`ScheduleID`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TRIP`
--

LOCK TABLES `TRIP` WRITE;
/*!40000 ALTER TABLE `TRIP` DISABLE KEYS */;
INSERT INTO `TRIP` VALUES (1,1,'2026-02-15','2008-05-00 00:00:00','0000-00-00 00:00:00','Completed'),(2,2,'2026-02-15','2009-02-00 00:00:00','0000-00-00 00:00:00','Completed'),(3,3,'2026-02-16','2010-05-00 00:00:00','2011-05-00 00:00:00','Completed'),(4,4,'2026-02-16','2011-02-00 00:00:00','0000-00-00 00:00:00','Completed'),(5,5,'2026-02-17','2012-05-00 00:00:00','0000-00-00 00:00:00','Completed'),(6,6,'2026-02-17','2013-03-00 00:00:00','0000-00-00 00:00:00','Completed'),(7,7,'2026-02-18','2014-02-00 00:00:00','0000-00-00 00:00:00','Completed'),(8,8,'2026-02-18','2015-04-00 00:00:00','0000-00-00 00:00:00','Completed'),(9,9,'2026-02-19','2016-05-00 00:00:00','0000-00-00 00:00:00','Completed'),(10,10,'2026-02-19','2017-02-00 00:00:00','0000-00-00 00:00:00','Completed');
/*!40000 ALTER TABLE `TRIP` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `USER_ACCOUNT`
--

DROP TABLE IF EXISTS `USER_ACCOUNT`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `USER_ACCOUNT` (
  `AccountID` int NOT NULL AUTO_INCREMENT,
  `MemberID` int NOT NULL,
  `Username` varchar(50) NOT NULL,
  `PasswordHash` varchar(255) NOT NULL, -- Storing hashed passwords is a security best practice
  `Role` ENUM('Admin', 'Regular User') NOT NULL DEFAULT 'Regular User',
  PRIMARY KEY (`AccountID`),
  UNIQUE KEY `Username` (`Username`),
  UNIQUE KEY `MemberID` (`MemberID`), -- Ensures a 1-to-1 relationship with MEMBER
  CONSTRAINT `USER_ACCOUNT_ibfk_1` FOREIGN KEY (`MemberID`) REFERENCES `MEMBER` (`MemberID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `USER_ACCOUNT`
--

LOCK TABLES `USER_ACCOUNT` WRITE;
/*!40000 ALTER TABLE `USER_ACCOUNT` DISABLE KEYS */;

-- Creating one Admin (MemberID 1) and one Regular User (MemberID 2) for testing purposes.
-- Passwords should be hashed in your actual Python backend, but we will insert placeholder hashes here.
-- Assume the raw password for both is 'password123'
INSERT INTO `USER_ACCOUNT` VALUES 
(1, 1, 'admin_rahul', 'password123', 'Admin'),
(2, 2, 'user_ananya', 'password123', 'Regular User');

/*!40000 ALTER TABLE `USER_ACCOUNT` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping events for database 'shuttle_system'
--





--
-- Dumping routines for database 'shuttle_system'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-15 16:29:21
