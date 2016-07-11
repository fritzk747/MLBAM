CREATE TABLE [dbo].[mlb_gameday_players](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[gameday_id] [int] NOT NULL,
	[first] [nvarchar](255) NULL DEFAULT (NULL),
	[last] [nvarchar](255) NULL DEFAULT (NULL),
	[number] [int] NULL DEFAULT (NULL),
	[boxname] [nvarchar](255) NULL DEFAULT (NULL),
	[position] [nvarchar](255) NULL DEFAULT (NULL),
	[current_position] [nvarchar](255) NULL DEFAULT (NULL),
	[throws] [nvarchar](255) NULL DEFAULT (NULL),
	[status] [nvarchar](255) NULL DEFAULT (NULL),
	[team_id] [nvarchar](255) NULL DEFAULT (NULL),
	[game_id] [nvarchar](255) NULL DEFAULT (NULL),
	[bats] [nvarchar](1) NULL DEFAULT (NULL),
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
