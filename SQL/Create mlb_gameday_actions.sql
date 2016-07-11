CREATE TABLE [dbo].[mlb_gameday_actions](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[balls] [int] NULL DEFAULT (NULL),
	[strikes] [int] NULL DEFAULT (NULL),
	[outs] [int] NULL DEFAULT (NULL),
	[des] [text] NULL,
	[event] [nvarchar](255) NULL DEFAULT (NULL),
	[player_id] [int] NULL DEFAULT (NULL),
	[pitch_number] [int] NULL DEFAULT (NULL),
	[pitcher_id] [int] NULL DEFAULT (NULL),
	[p_throws] [nvarchar](1) NULL DEFAULT (NULL),
	[game_id] [nvarchar](255) NULL DEFAULT (NULL),
	[event_num] [int] NULL,
	[at_bat_num] [int] NULL,
	[catcher_id] [int] NULL DEFAULT (NULL),
	[pit_retro_manager_id] [nvarchar](255) NULL DEFAULT (NULL),
	[bat_retro_manager_id] [nvarchar](255) NULL DEFAULT (NULL),
	[home_away_binary] [tinyint] NULL DEFAULT (NULL),
	[stand] [nvarchar](255) NULL DEFAULT (NULL),
	[tfs_zulu] [nvarchar](255) NULL DEFAULT (NULL),
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
