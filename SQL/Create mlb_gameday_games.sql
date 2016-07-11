CREATE TABLE [dbo].[mlb_gameday_games](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[gid] [nvarchar](255) NULL DEFAULT (NULL),
	[date] [date] NULL DEFAULT (NULL),
	[home_id] [nvarchar](3) NULL DEFAULT (NULL),
	[away_id] [nvarchar](3) NULL DEFAULT (NULL),
	[game_num] [int] NULL DEFAULT (NULL),
	[umpire_hp_id] [int] NULL DEFAULT (NULL),
	[umpire_1b_id] [int] NULL DEFAULT (NULL),
	[umpire_2b_id] [int] NULL DEFAULT (NULL),
	[umpire_3b_id] [int] NULL DEFAULT (NULL),
	[wind] [nvarchar](100) NULL DEFAULT (NULL),
	[wind_speed] [int] NULL DEFAULT (NULL),
	[wind_dir] [nvarchar](255) NULL DEFAULT (NULL),
	[temp] [int] NULL DEFAULT (NULL),
	[game_type] [nvarchar](255) NULL DEFAULT (NULL),
	[stadium_id] [int] NULL DEFAULT (NULL),
	[dome] [nvarchar](255) NULL DEFAULT ('dome'),
	[home_retro_manager_id] [nvarchar](255) NULL DEFAULT (NULL),
	[away_retro_manager_id] [nvarchar](255) NULL DEFAULT (NULL),
	[sport_code] [nvarchar](50) NULL DEFAULT (NULL),
	[league] [nvarchar](50) NULL DEFAULT (NULL),
	[league_id] [int] NULL DEFAULT (NULL),
	[home_dh_id] [int] NULL DEFAULT (NULL),
	[away_dh_id] [int] NULL DEFAULT (NULL),
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

