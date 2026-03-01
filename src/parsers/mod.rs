pub mod generic;
pub mod linktarget;
pub mod page;
pub mod pagelinks;
pub mod schema;

macro_rules! define_generic_table_parser {
    ($module_name:ident, $type_name:ident, $table_variant:ident) => {
        pub mod $module_name {
            use std::io::BufRead;

            use super::generic::{GenericRow, TableRowsIter, iter_table_rows};
            use super::schema::WikipediaTable;

            pub type $type_name = GenericRow;

            pub fn iter_rows<R: BufRead>(reader: R) -> TableRowsIter<R> {
                iter_table_rows(reader, WikipediaTable::$table_variant)
            }
        }
    };
}

define_generic_table_parser!(actor, ActorRow, Actor);
define_generic_table_parser!(archive, ArchiveRow, Archive);
define_generic_table_parser!(block, BlockRow, Block);
define_generic_table_parser!(block_target, BlockTargetRow, BlockTarget);
define_generic_table_parser!(bot_passwords, BotPasswordsRow, BotPasswords);
define_generic_table_parser!(category, CategoryRow, Category);
define_generic_table_parser!(categorylinks, CategoryLinksRow, CategoryLinks);
define_generic_table_parser!(change_tag, ChangeTagRow, ChangeTag);
define_generic_table_parser!(change_tag_def, ChangeTagDefRow, ChangeTagDef);
define_generic_table_parser!(collation, CollationRow, Collation);
define_generic_table_parser!(comment, CommentRow, Comment);
define_generic_table_parser!(content, ContentRow, Content);
define_generic_table_parser!(content_models, ContentModelsRow, ContentModels);
define_generic_table_parser!(existencelinks, ExistenceLinksRow, ExistenceLinks);
define_generic_table_parser!(externallinks, ExternalLinksRow, ExternalLinks);
define_generic_table_parser!(file, FileRow, File);
define_generic_table_parser!(filearchive, FileArchiveRow, FileArchive);
define_generic_table_parser!(filerevision, FileRevisionRow, FileRevision);
define_generic_table_parser!(filetypes, FileTypesRow, FileTypes);
define_generic_table_parser!(image, ImageRow, Image);
define_generic_table_parser!(imagelinks, ImageLinksRow, ImageLinks);
define_generic_table_parser!(interwiki, InterwikiRow, Interwiki);
define_generic_table_parser!(ip_changes, IpChangesRow, IpChanges);
define_generic_table_parser!(
    ipblocks_restrictions,
    IpblocksRestrictionsRow,
    IpblocksRestrictions
);
define_generic_table_parser!(iwlinks, IwLinksRow, IwLinks);
define_generic_table_parser!(job, JobRow, Job);
define_generic_table_parser!(l10n_cache, L10nCacheRow, L10nCache);
define_generic_table_parser!(langlinks, LangLinksRow, LangLinks);
define_generic_table_parser!(log_search, LogSearchRow, LogSearch);
define_generic_table_parser!(logging, LoggingRow, Logging);
define_generic_table_parser!(objectcache, ObjectCacheRow, ObjectCache);
define_generic_table_parser!(oldimage, OldImageRow, OldImage);
define_generic_table_parser!(page_props, PagePropsRow, PageProps);
define_generic_table_parser!(page_restrictions, PageRestrictionsRow, PageRestrictions);
define_generic_table_parser!(protected_titles, ProtectedTitlesRow, ProtectedTitles);
define_generic_table_parser!(querycache, QueryCacheRow, QueryCache);
define_generic_table_parser!(querycache_info, QueryCacheInfoRow, QueryCacheInfo);
define_generic_table_parser!(querycachetwo, QueryCacheTwoRow, QueryCacheTwo);
define_generic_table_parser!(recentchanges, RecentChangesRow, RecentChanges);
define_generic_table_parser!(redirect, RedirectRow, Redirect);
define_generic_table_parser!(revision, RevisionRow, Revision);
define_generic_table_parser!(searchindex, SearchIndexRow, SearchIndex);
define_generic_table_parser!(site_identifiers, SiteIdentifiersRow, SiteIdentifiers);
define_generic_table_parser!(site_stats, SiteStatsRow, SiteStats);
define_generic_table_parser!(sites, SitesRow, Sites);
define_generic_table_parser!(slot_roles, SlotRolesRow, SlotRoles);
define_generic_table_parser!(slots, SlotsRow, Slots);
define_generic_table_parser!(templatelinks, TemplateLinksRow, TemplateLinks);
define_generic_table_parser!(text, TextRow, Text);
define_generic_table_parser!(updatelog, UpdateLogRow, UpdateLog);
define_generic_table_parser!(uploadstash, UploadStashRow, UploadStash);
define_generic_table_parser!(user, UserRow, User);
define_generic_table_parser!(
    user_autocreate_serial,
    UserAutocreateSerialRow,
    UserAutocreateSerial
);
define_generic_table_parser!(user_former_groups, UserFormerGroupsRow, UserFormerGroups);
define_generic_table_parser!(user_groups, UserGroupsRow, UserGroups);
define_generic_table_parser!(user_newtalk, UserNewTalkRow, UserNewTalk);
define_generic_table_parser!(user_properties, UserPropertiesRow, UserProperties);
define_generic_table_parser!(watchlist, WatchlistRow, Watchlist);
define_generic_table_parser!(watchlist_expiry, WatchlistExpiryRow, WatchlistExpiry);
define_generic_table_parser!(watchlist_label, WatchlistLabelRow, WatchlistLabel);
define_generic_table_parser!(
    watchlist_label_member,
    WatchlistLabelMemberRow,
    WatchlistLabelMember
);
