use clap::{Args, Parser, Subcommand};

/// Query the database for artists, albums, genres, and tracks.
#[derive(Parser, Debug)]
pub struct QueryArgs {
    #[command(subcommand)]
    pub command: QueryCommand,
}

#[derive(Subcommand, Debug)]
pub enum QueryCommand {
    /// Search for an artist by name.
    Artist(ArtistArgs),
    /// Find a genre by name.
    Genre(GenreArgs),
    /// Search for an album by name.
    Album(AlbumArgs),
    /// Search across artists, albums, and tracks simultaneously.
    Search(SearchArgs),
}

/// Search for an artist by name.
#[derive(Args, Debug)]
pub struct ArtistArgs {
    /// Full-text search term for artist name.
    #[arg(long, short, required = true)]
    pub name: String,
}

/// Find a genre by name.
#[derive(Args, Debug)]
pub struct GenreArgs {
    /// Full-text search term for genre name.
    #[arg(long, short, required = true)]
    pub name: String,
    /// Maximum number of results to return.
    #[arg(long, default_value = "20")]
    pub limit: usize,
    /// Number of results to skip (for pagination).
    #[arg(long, default_value = "0")]
    pub offset: usize,
}

/// Search for an album by name.
#[derive(Args, Debug)]
pub struct AlbumArgs {
    /// Full-text search term for album name.
    #[arg(long, short, required = true)]
    pub name: String,
}

/// Search across artists, albums, and tracks simultaneously.
#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Search term to match against artist, album, and track names.
    #[arg(long, short = 't', required = true)]
    pub term: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_artist() {
        let args = QueryArgs::try_parse_from(["query", "artist", "--name", "Miles Davis"]).unwrap();
        match &args.command {
            QueryCommand::Artist(a) => assert_eq!(a.name, "Miles Davis"),
            _ => panic!("expected artist command"),
        }
    }

    #[test]
    fn test_query_genre_defaults() {
        let args = QueryArgs::try_parse_from(["query", "genre", "--name", "Jazz"]).unwrap();
        match &args.command {
            QueryCommand::Genre(g) => {
                assert_eq!(g.name, "Jazz");
                assert_eq!(g.limit, 20);
                assert_eq!(g.offset, 0);
            }
            _ => panic!("expected genre command"),
        }
    }

    #[test]
    fn test_query_genre_pagination() {
        let args = QueryArgs::try_parse_from([
            "query", "genre", "--name", "Rock", "--limit", "10", "--offset", "20",
        ])
        .unwrap();
        match &args.command {
            QueryCommand::Genre(g) => {
                assert_eq!(g.name, "Rock");
                assert_eq!(g.limit, 10);
                assert_eq!(g.offset, 20);
            }
            _ => panic!("expected genre command"),
        }
    }

    #[test]
    fn test_query_album() {
        let args = QueryArgs::try_parse_from(["query", "album", "--name", "Kind of Blue"]).unwrap();
        match &args.command {
            QueryCommand::Album(a) => assert_eq!(a.name, "Kind of Blue"),
            _ => panic!("expected album command"),
        }
    }

    #[test]
    fn test_query_search() {
        let args = QueryArgs::try_parse_from(["query", "search", "--term", "Miles"]).unwrap();
        match &args.command {
            QueryCommand::Search(s) => assert_eq!(s.term, "Miles"),
            _ => panic!("expected search command"),
        }
    }

    #[test]
    fn test_query_artist_requires_name() {
        let result = QueryArgs::try_parse_from(["query", "artist"]);
        assert!(result.is_err());
    }
}
