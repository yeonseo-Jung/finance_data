class Dart:
    info = [
        
    ]
    
    quarters_q = [
        'Q202211013', 
        'Q202111011', 'Q202111014', 'Q202111012', 'Q202111013',
        'Q202011011', 'Q202011014', 'Q202011012', 'Q202011013',
        'Q201911011', 'Q201911014', 'Q201911012', 'Q201911013',
        'Q201811011', 'Q201811014', 'Q201811012', 'Q201811013',
    ]
    
    quarters = [
        'Q202211013', 
        'Y202111011', 'Q202111014', 'Q202111012', 'Q202111013',
        'Y202011011', 'Q202011014', 'Q202011012', 'Q202011013',
        'Y201911011', 'Q201911014', 'Q201911012', 'Q201911013',
        'Y201811011', 'Q201811014', 'Q201811012', 'Q201811013',
    ]
    
    query_dict = {
        'dart_finstatements': f'CREATE TABLE `dart_finstatements` (\
        `rcept_no` varchar(20),\
        `reprt_code` varchar(20),\
        `corp_code` varchar(20),\
        `bsns_year` varchar(20),\
        `fs_div` varchar(20),\
        `sj_div` varchar(20),\
        `sj_nm` varchar(20),\
        `stock_code` varchar(20),\
        `account_id` varchar(255),\
        `account_nm` varchar(255),\
        `thstrm_nm` varchar(20),\
        `thstrm_amount` float DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_amounts_all': f'CREATE TABLE `dart_amounts_all` (\
        `stock_code` varchar(20),\
        `corp_code` varchar(20),\
        `fs_div` varchar(20),\
        `sj_div` varchar(20),\
        `account_id` varchar(255),\
        `account_nm` text,\
        `Q202211013` float DEFAULT NULL,\
        `Y202111011` float DEFAULT NULL,\
        `Q202111014` float DEFAULT NULL,\
        `Q202111012` float DEFAULT NULL,\
        `Q202111013` float DEFAULT NULL,\
        `Y202011011` float DEFAULT NULL,\
        `Q202011014` float DEFAULT NULL,\
        `Q202011012` float DEFAULT NULL,\
        `Q202011013` float DEFAULT NULL,\
        `Y201911011` float DEFAULT NULL,\
        `Q201911014` float DEFAULT NULL,\
        `Q201911012` float DEFAULT NULL,\
        `Q201911013` float DEFAULT NULL,\
        `Y201811011` float DEFAULT NULL,\
        `Q201811014` float DEFAULT NULL,\
        `Q201811012` float DEFAULT NULL,\
        `Q201811013` float DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_amounts': f'CREATE TABLE `dart_amounts` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `account_nm_eng` varchar(255),\
        `account_id` varchar(255),\
        `account_nm_kor` varchar(255),\
        `stock_code` varchar(20),\
        `fs_div` varchar(20),\
        `sj_div` varchar(20),\
        `Q202211013` float DEFAULT NULL,\
        `Q202111011` float DEFAULT NULL,\
        `Q202111014` float DEFAULT NULL,\
        `Q202111012` float DEFAULT NULL,\
        `Q202111013` float DEFAULT NULL,\
        `Q202011011` float DEFAULT NULL,\
        `Q202011014` float DEFAULT NULL,\
        `Q202011012` float DEFAULT NULL,\
        `Q202011013` float DEFAULT NULL,\
        `Q201911011` float DEFAULT NULL,\
        `Q201911014` float DEFAULT NULL,\
        `Q201911012` float DEFAULT NULL,\
        `Q201911013` float DEFAULT NULL,\
        `Q201811011` float DEFAULT NULL,\
        `Q201811014` float DEFAULT NULL,\
        `Q201811012` float DEFAULT NULL,\
        `Q201811013` float DEFAULT NULL,\
        `created` datetime DEFAULT NULL,\
        `updated` datetime DEFAULT NULL,\
        PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_annualized': f'CREATE TABLE `dart_annualized` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `account_nm_eng` varchar(255),\
        `account_id` varchar(255),\
        `account_nm_kor` varchar(255),\
        `stock_code` varchar(20),\
        `fs_div` varchar(20),\
        `sj_div` varchar(20),\
        `Q202211013` float DEFAULT NULL,\
        `Q202111011` float DEFAULT NULL,\
        `Q202111014` float DEFAULT NULL,\
        `Q202111012` float DEFAULT NULL,\
        `Q202111013` float DEFAULT NULL,\
        `Q202011011` float DEFAULT NULL,\
        `Q202011014` float DEFAULT NULL,\
        `Q202011012` float DEFAULT NULL,\
        `Q202011013` float DEFAULT NULL,\
        `Q201911011` float DEFAULT NULL,\
        `Q201911014` float DEFAULT NULL,\
        `Q201911012` float DEFAULT NULL,\
        `Q201911013` float DEFAULT NULL,\
        `Q201811011` float DEFAULT NULL,\
        PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_annualized_octa': f'CREATE TABLE `dart_annualized_octa` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `account_nm_eng` varchar(255),\
        `account_id` varchar(255),\
        `account_nm_kor` varchar(255),\
        `stock_code` varchar(20),\
        `fs_div` varchar(20),\
        `sj_div` varchar(20),\
        `Q202211013` float DEFAULT NULL,\
        `Q202111011` float DEFAULT NULL,\
        `Q202111014` float DEFAULT NULL,\
        `Q202111012` float DEFAULT NULL,\
        `Q202111013` float DEFAULT NULL,\
        `Q202011011` float DEFAULT NULL,\
        `Q202011014` float DEFAULT NULL,\
        `Q202011012` float DEFAULT NULL,\
        `Q202011013` float DEFAULT NULL,\
        `Q201911011` float DEFAULT NULL,\
        PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_ratios': f'CREATE TABLE `dart_ratios` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `stock_code` varchar(20),\
        `ratio` varchar(255),\
        `Q202211013` float DEFAULT NULL,\
        `Q202111011` float DEFAULT NULL,\
        `Q202111014` float DEFAULT NULL,\
        `Q202111012` float DEFAULT NULL,\
        `Q202111013` float DEFAULT NULL,\
        `Q202011011` float DEFAULT NULL,\
        `Q202011014` float DEFAULT NULL,\
        `Q202011012` float DEFAULT NULL,\
        `Q202011013` float DEFAULT NULL,\
        `Q201911011` float DEFAULT NULL,\
        `Q201911014` float DEFAULT NULL,\
        `Q201911012` float DEFAULT NULL,\
        `Q201911013` float DEFAULT NULL,\
        `Q201811011` float DEFAULT NULL,\
        `Q201811014` float DEFAULT NULL,\
        `Q201811012` float DEFAULT NULL,\
        `Q201811013` float DEFAULT NULL,\
        `created` datetime DEFAULT NULL,\
        `updated` datetime DEFAULT NULL,\
        PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_accounts': f'CREATE TABLE `dart_accounts` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `account_nm_eng` varchar(255),\
        `account_id` varchar(255),\
        `account_nm_kor` varchar(255),\
        PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'dart_accounts_all': f'CREATE TABLE `dart_accounts_all` (\
        `sj_div` varchar(20),\
        `account_id` varchar(255),\
        `account_nm` text\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',

        'fnguide_ratio': f'CREATE TABLE `fnguide_ratio` (\
        `stock_code` varchar(20),\
        `ratio` varchar(255),\
        `Y_4` float DEFAULT NULL,\
        `Y_3` float DEFAULT NULL,\
        `Y_2` float DEFAULT NULL,\
        `Y_1` float DEFAULT NULL,\
        `CLE` float DEFAULT NULL,\
        `note` varchar(20)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;',
    }