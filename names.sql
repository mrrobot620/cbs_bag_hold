-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: localhost
-- Generation Time: Nov 26, 2023 at 08:33 AM
-- Server version: 10.4.28-MariaDB
-- PHP Version: 8.0.28

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `pendency`
--

-- --------------------------------------------------------

--
-- Table structure for table `names`
--

CREATE TABLE `names` (
  `PPPH (YKB)` varchar(255) NOT NULL,
  `YKB FDP PENDENCY REPORT LIVE` int(11) NOT NULL,
  `LIVE` int(11) NOT NULL,
  `PPPH (ZO)` int(11) NOT NULL,
  `PPPH (B5)` int(11) NOT NULL,
  `TOTAL` int(11) NOT NULL,
  `PH` int(11) NOT NULL,
  `SPH` int(11) NOT NULL,
  `12-24` int(11) NOT NULL,
  `24-48` int(11) NOT NULL,
  `>48` int(11) NOT NULL,
  `>12` int(11) NOT NULL,
  `OTHER MH` int(11) NOT NULL,
  `OB` int(11) NOT NULL,
  `OB CROSSDOCK` int(11) NOT NULL,
  `OB SEMI-LARGE` int(11) NOT NULL,
  `SECONDARY PENDING` int(11) NOT NULL,
  `SECONDARY (ZO)` int(11) NOT NULL,
  `SECONDARY (B5)` int(11) NOT NULL,
  `BAGGING PENDING` int(11) NOT NULL,
  `BAGGING (ZO)` int(11) NOT NULL,
  `BAGGING (B5)` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `names`
--

INSERT INTO `names` (`PPPH (YKB)`, `YKB FDP PENDENCY REPORT LIVE`, `LIVE`, `PPPH (ZO)`, `PPPH (B5)`, `TOTAL`, `PH`, `SPH`, `12-24`, `24-48`, `>48`, `>12`, `OTHER MH`, `OB`, `OB CROSSDOCK`, `OB SEMI-LARGE`, `SECONDARY PENDING`, `SECONDARY (ZO)`, `SECONDARY (B5)`, `BAGGING PENDING`, `BAGGING (ZO)`, `BAGGING (B5)`) VALUES
('PPPH (YKB)', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
('PPPH (YKB)', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
