
use ccom4017_dfs_g02;

CREATE TABLE `inode` (
  `fid` int(11) NOT NULL auto_increment,
  `fname` varchar(100) NOT NULL default ' ',
  `fsize` int(11) NOT NULL default '0',
  PRIMARY KEY  (`fid`),
  UNIQUE KEY (`fname`) 
) ENGINE=MyISAM ;

CREATE TABLE `data_node` (
  `nid` int(11) NOT NULL auto_increment,
  `name` varchar(30) NOT NULL default ' ',
  `address` varchar(30) NOT NULL default ' ',
  `port` int(11) NOT NULL default '0',
  PRIMARY KEY  (`nid`),
  UNIQUE KEY (`address`, `port`),
  UNIQUE KEY (`name`)
) ENGINE=MyISAM ;

CREATE TABLE `block` (
  `bid` int(11) NOT NULL auto_increment,
  `fid` int(11) NOT NULL default '0',
  `nid` int(11) NOT NULL default '0',
  `cid` int(11) NOT NULL default '0',
  PRIMARY KEY  (`bid`),
  UNIQUE KEY  (`nid`, `cid`) 
) ENGINE=MyISAM ;
