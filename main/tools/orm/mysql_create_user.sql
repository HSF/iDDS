#create database ess DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
create database ess DEFAULT CHARACTER SET latin1  DEFAULT COLLATE latin1_general_ci;


CREATE USER 'ess'@'%' IDENTIFIED BY 'ess_passwd';
GRANT ALL PRIVILEGES ON ess.* TO 'ess'@'%';
CREATE USER 'ess'@'localhost' IDENTIFIED BY 'ess_passwd';
GRANT ALL PRIVILEGES ON ess.* TO 'ess'@'localhost';
flush PRIVILEGES;
