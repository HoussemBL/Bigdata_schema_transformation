package de.mindlab.spark_schema_transformer.row_iterator.example_definitions

import de.mindlab.spark_schema_transformer.row_iterator._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._

object ObjectIdTransformation {
  
  
val oPathStructSrc_0001 = PathDefinition(Seq(PathToken("nm_assisted_Goals")                                                                                    ))  // nm_assisted_Goals.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0002 = PathDefinition(Seq(PathToken("nm_assisted_Processes")                                                                                ))  // nm_assisted_Processes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0003 = PathDefinition(Seq(PathToken("nm_assisted_Processes"),                         PathToken("Attributes")                               ))  //  nm_assisted_Processes.list.element.Attributes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0004 = PathDefinition(Seq(PathToken("nm_backwarded_goal_Goals")                                                                             ))  // nm_backwarded_goal_Goals.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0005 = PathDefinition(Seq(PathToken("nm_backwarded_process_Processes")                                                                      ))  // nm_backwarded_process_Processes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0006 = PathDefinition(Seq(PathToken("nm_backwarded_process_Processes"),               PathToken("Attributes")                               ))  // nm_backwarded_process_Processes.list.element.Attributes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0007 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads")                                                                      ))  // nm_content_PageContentDownloads.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0008 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("Events")                                   ))  // nm_content_PageContentDownloads.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0009 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentDownloads.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0010 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("Searches")                                 ))  // nm_content_PageContentDownloads.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0011 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentDownloads.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0012 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentDownloads.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0013 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentDownloads.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0014 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("TargetGroups")                             ))  // nm_content_PageContentDownloads.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0015 = PathDefinition(Seq(PathToken("nm_content_PageContentDownloads"),               PathToken("TargetPages")                              ))  // nm_content_PageContentDownloads.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0016 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks")                                                                  ))  // nm_content_PageContentExternalLinks.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0017 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("Events")                                   ))  // nm_content_PageContentExternalLinks.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0018 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentExternalLinks.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0019 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("Searches")                                 ))  // nm_content_PageContentExternalLinks.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0020 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentExternalLinks.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0021 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentExternalLinks.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0022 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentExternalLinks.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0023 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("TargetGroups")                             ))  // nm_content_PageContentExternalLinks.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0024 = PathDefinition(Seq(PathToken("nm_content_PageContentExternalLinks"),           PathToken("TargetPages")                              ))  // nm_content_PageContentExternalLinks.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0025 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments")                                                                      ))  // nm_content_PageContentFragments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0026 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("Events")                                   ))  // nm_content_PageContentFragments.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0027 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentFragments.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0028 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("Searches")                                 ))  // nm_content_PageContentFragments.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0029 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentFragments.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0030 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentFragments.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0031 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentFragments.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0032 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("TargetGroups")                             ))  // nm_content_PageContentFragments.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0033 = PathDefinition(Seq(PathToken("nm_content_PageContentFragments"),               PathToken("TargetPages")                              ))  // nm_content_PageContentFragments.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0034 = PathDefinition(Seq(PathToken("nm_content_PageContent")                                                                               ))  // nm_content_PageContent.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0035 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("Events")                                   ))  // nm_content_PageContent.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0036 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContent.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0037 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("Searches")                                 ))  // nm_content_PageContent.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0038 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContent.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0039 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContent.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0040 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContent.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0041 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("TargetGroups")                             ))  // nm_content_PageContent.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0042 = PathDefinition(Seq(PathToken("nm_content_PageContent"),                        PathToken("TargetPages")                              ))  // nm_content_PageContent.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0043 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia")                                                                          ))  // nm_content_PageContentMedia.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0044 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("Events")                                   ))  // nm_content_PageContentMedia.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0045 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentMedia.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0046 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("Searches")                                 ))  // nm_content_PageContentMedia.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0047 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentMedia.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0048 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentMedia.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0049 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentMedia.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0050 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("TargetGroups")                             ))  // nm_content_PageContentMedia.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0051 = PathDefinition(Seq(PathToken("nm_content_PageContentMedia"),                   PathToken("TargetPages")                              ))  // nm_content_PageContentMedia.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0052 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite")                                                                         ))  // nm_content_PageContentOnSite.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0053 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("Events")                                   ))  // nm_content_PageContentOnSite.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0054 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentOnSite.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0056 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("Searches")                                 ))  // nm_content_PageContentOnSite.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0057 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentOnSite.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0058 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentOnSite.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0059 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentOnSite.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0060 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("TargetGroups")                             ))  // nm_content_PageContentOnSite.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0061 = PathDefinition(Seq(PathToken("nm_content_PageContentOnSite"),                  PathToken("TargetPages")                              ))  // nm_content_PageContentOnSite.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0062 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches")                                                                       ))  // nm_content_PageContentSearches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0063 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("Events")                                   ))  // nm_content_PageContentSearches.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0064 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("MediaData"),          PathToken("Segments")))  // nm_content_PageContentSearches.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0065 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("Searches")                                 ))  // nm_content_PageContentSearches.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0066 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("Searches"),           PathToken("Filters") ))  // nm_content_PageContentSearches.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0067 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("Searches"),           PathToken("Sorting") ))  // nm_content_PageContentSearches.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0068 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("Searches"),           PathToken("Usages")  ))  // nm_content_PageContentSearches.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0069 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("TargetGroups")                             ))  // nm_content_PageContentSearches.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0070 = PathDefinition(Seq(PathToken("nm_content_PageContentSearches"),                PathToken("TargetPages")                              ))  // nm_content_PageContentSearches.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0071 = PathDefinition(Seq(PathToken("nm_content_Page.Events")                                                                               ))  // nm_content_Page.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0072 = PathDefinition(Seq(PathToken("nm_ecommerce_Purchase.Merchants")                                                                      ))  // nm_ecommerce_Purchase.Merchants.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0073 = PathDefinition(Seq(PathToken("nm_ecommerce_Purchase.Products")                                                                       ))  // nm_ecommerce_Purchase.Products.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0074 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads")                                                            ))  // nm_forwarded_content_PageContentDownloads.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0075 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("Events")                                   ))  // nm_forwarded_content_PageContentDownloads.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0076 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentDownloads.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0077 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentDownloads.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0078 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentDownloads.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0079 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentDownloads.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0080 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentDownloads.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0081 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentDownloads.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0082 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentDownloads"),     PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentDownloads.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0083 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks")                                                        ))  // nm_forwarded_content_PageContentExternalLinks.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0084 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("Events")                                   ))  // nm_forwarded_content_PageContentExternalLinks.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0085 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentExternalLinks.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0086 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentExternalLinks.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0087 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentExternalLinks.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0088 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentExternalLinks.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0089 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentExternalLinks.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0090 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentExternalLinks.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0091 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentExternalLinks"), PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentExternalLinks.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0092 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments")                                                            ))  // nm_forwarded_content_PageContentFragments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0093 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("Events")                                   ))  // nm_forwarded_content_PageContentFragments.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0094 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentFragments.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0095 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentFragments.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0096 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentFragments.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0097 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentFragments.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0098 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentFragments.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0099 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentFragments.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0100 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentFragments"),     PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentFragments.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0101 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent")                                                                     ))  // nm_forwarded_content_PageContent.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0102 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("Events")                                   ))  // nm_forwarded_content_PageContent.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0103 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContent.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0104 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("Searches")                                 ))  // nm_forwarded_content_PageContent.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0105 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContent.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0106 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContent.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0107 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContent.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0108 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContent.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0109 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContent"),              PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContent.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0110 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia")                                                                ))  // nm_forwarded_content_PageContentMedia.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0111 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("Events")                                   ))  // nm_forwarded_content_PageContentMedia.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0112 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentMedia.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0113 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentMedia.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0114 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentMedia.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0115 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentMedia.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0116 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentMedia.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0117 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentMedia.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0118 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentMedia"),         PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentMedia.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0119 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite")                                                               ))  // nm_forwarded_content_PageContentOnSite.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0120 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("Events")                                   ))  // nm_forwarded_content_PageContentOnSite.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0121 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentOnSite.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0122 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentOnSite.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0123 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentOnSite.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0124 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentOnSite.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0125 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentOnSite.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0126 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentOnSite.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0127 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentOnSite"),        PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentOnSite.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0128 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches")                                                             ))  // nm_forwarded_content_PageContentSearches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0129 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("Events")                                   ))  // nm_forwarded_content_PageContentSearches.list.element.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0130 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("MediaData"),          PathToken("Segments")))  // nm_forwarded_content_PageContentSearches.list.element.MediaData.Segments.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0131 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("Searches")                                 ))  // nm_forwarded_content_PageContentSearches.list.element.Searches.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0132 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("Searches"),           PathToken("Filters") ))  // nm_forwarded_content_PageContentSearches.list.element.Searches.list.element.Filters.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0133 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("Searches"),           PathToken("Sorting") ))  // nm_forwarded_content_PageContentSearches.list.element.Searches.list.element.Sorting.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0134 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("Searches"),           PathToken("Usages")  ))  // nm_forwarded_content_PageContentSearches.list.element.Searches.list.element.Usages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0135 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("TargetGroups")                             ))  // nm_forwarded_content_PageContentSearches.list.element.TargetGroups.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0136 = PathDefinition(Seq(PathToken("nm_forwarded_content_PageContentSearches"),      PathToken("TargetPages")                              ))  // nm_forwarded_content_PageContentSearches.list.element.TargetPages.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0137 = PathDefinition(Seq(PathToken("nm_forwarded_goal_Goals")                                                                              ))  // nm_forwarded_goal_Goals.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0138 = PathDefinition(Seq(PathToken("nm_goal_Goals")                                                                                        ))  // nm_goal_Goals.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0139 = PathDefinition(Seq(PathToken("nm_inflow_Inflow.Campaign.LandingPage.Events")                                                         ))  // nm_inflow_Inflow.Campaign.LandingPage.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0140 = PathDefinition(Seq(PathToken("nm_process_Processes")                                                                                 ))  // nm_process_Processes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0141 = PathDefinition(Seq(PathToken("nm_process_Processes"),                          PathToken("Attributes")                               ))  // nm_process_Processes.list.element.Attributes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0142 = PathDefinition(Seq(PathToken("nm_session_Session.EntryPage.Events")                                                                  ))  // nm_session_Session.EntryPage.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0143 = PathDefinition(Seq(PathToken("nm_session_Session.ExitPage.Events")                                                                   ))  // nm_session_Session.ExitPage.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0144 = PathDefinition(Seq(PathToken("prv_nm_content_Page.Events")                                                                           ))  // prv_nm_content_Page.Events.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0145 = PathDefinition(Seq(PathToken("prv_nm_process_Processes")                                                                             ))  // prv_nm_process_Processes.list.element.ObjectId (BYTE_ARRAY)
val oPathStructSrc_0146 = PathDefinition(Seq(PathToken("prv_nm_process_Processes"),                      PathToken("Attributes")                               ))  // prv_nm_process_Processes.list.element.Attributes.list.element.ObjectId (BYTE_ARRAY)

// T => ARRAY[T]:
val oPathStructSrc_0501 = PathDefinition(Seq(PathToken("nm_process_Processes"),                          PathToken("Error")                                    ))  //nm_process_Processes.list.element.Error
val oPathStructSrc_0502 = PathDefinition(Seq(PathToken("nm_process_Processes"),                          PathToken("Help")                                     ))  //nm_process_Processes.list.element.Help
val oPathStructSrc_0503 = PathDefinition(Seq(PathToken("nm_assisted_Processes"),                         PathToken("Error")                                    ))  //nm_assisted_Processes.list.element.Error
val oPathStructSrc_0504 = PathDefinition(Seq(PathToken("nm_assisted_Processes"),                         PathToken("Help")                                     ))  //nm_assisted_Processes.list.element.Help
val oPathStructSrc_0505 = PathDefinition(Seq(PathToken("nm_backwarded_process_Processes"),               PathToken("Error")                                    ))  //nm_backwarded_process_Processes.list.element.Error
val oPathStructSrc_0506 = PathDefinition(Seq(PathToken("nm_backwarded_process_Processes"),               PathToken("Help")                                     ))  //nm_backwarded_process_Processes.list.element.Help
val oPathStructSrc_0507 = PathDefinition(Seq(PathToken("prv_nm_process_Processes"),                      PathToken("Error")                                    ))  //prv_nm_process_Processes.list.element.Error
val oPathStructSrc_0508 = PathDefinition(Seq(PathToken("prv_nm_process_Processes"),                      PathToken("Help")                                     ))  //prv_nm_process_Processes.list.element.Help





val oPathStructSrc_0000 = PathDefinition(Seq(PathToken("nm_pageView_id")                                                                                       ))

val oSrcDef_0000 = SourceDefinition( null, ConstantDefinition( "ObjectId", 0 ) )
val oSrcDef_1000 = SourceDefinition( oPathStructSrc_0000 )

val oSrcDef_0001 = SourceDefinition( oPathStructSrc_0001 )
val oSrcDef_0002 = SourceDefinition( oPathStructSrc_0002 )
val oSrcDef_0003 = SourceDefinition( oPathStructSrc_0003 )
val oSrcDef_0004 = SourceDefinition( oPathStructSrc_0004 )
val oSrcDef_0005 = SourceDefinition( oPathStructSrc_0005 )
val oSrcDef_0006 = SourceDefinition( oPathStructSrc_0006 )
val oSrcDef_0007 = SourceDefinition( oPathStructSrc_0007 )
val oSrcDef_0008 = SourceDefinition( oPathStructSrc_0008 )
val oSrcDef_0009 = SourceDefinition( oPathStructSrc_0009 )
val oSrcDef_0010 = SourceDefinition( oPathStructSrc_0010 )
val oSrcDef_0011 = SourceDefinition( oPathStructSrc_0011 )
val oSrcDef_0012 = SourceDefinition( oPathStructSrc_0012 )
val oSrcDef_0013 = SourceDefinition( oPathStructSrc_0013 )
val oSrcDef_0014 = SourceDefinition( oPathStructSrc_0014 )
val oSrcDef_0015 = SourceDefinition( oPathStructSrc_0015 )
val oSrcDef_0016 = SourceDefinition( oPathStructSrc_0016 )
val oSrcDef_0017 = SourceDefinition( oPathStructSrc_0017 )
val oSrcDef_0018 = SourceDefinition( oPathStructSrc_0018 )
val oSrcDef_0019 = SourceDefinition( oPathStructSrc_0019 )
val oSrcDef_0020 = SourceDefinition( oPathStructSrc_0020 )
val oSrcDef_0021 = SourceDefinition( oPathStructSrc_0021 )
val oSrcDef_0022 = SourceDefinition( oPathStructSrc_0022 )
val oSrcDef_0023 = SourceDefinition( oPathStructSrc_0023 )
val oSrcDef_0024 = SourceDefinition( oPathStructSrc_0024 )
val oSrcDef_0025 = SourceDefinition( oPathStructSrc_0025 )
val oSrcDef_0026 = SourceDefinition( oPathStructSrc_0026 )
val oSrcDef_0027 = SourceDefinition( oPathStructSrc_0027 )
val oSrcDef_0028 = SourceDefinition( oPathStructSrc_0028 )
val oSrcDef_0029 = SourceDefinition( oPathStructSrc_0029 )
val oSrcDef_0030 = SourceDefinition( oPathStructSrc_0030 )
val oSrcDef_0031 = SourceDefinition( oPathStructSrc_0031 )
val oSrcDef_0032 = SourceDefinition( oPathStructSrc_0032 )
val oSrcDef_0033 = SourceDefinition( oPathStructSrc_0033 )
val oSrcDef_0034 = SourceDefinition( oPathStructSrc_0034 )
val oSrcDef_0035 = SourceDefinition( oPathStructSrc_0035 )
val oSrcDef_0036 = SourceDefinition( oPathStructSrc_0036 )
val oSrcDef_0037 = SourceDefinition( oPathStructSrc_0037 )
val oSrcDef_0038 = SourceDefinition( oPathStructSrc_0038 )
val oSrcDef_0039 = SourceDefinition( oPathStructSrc_0039 )
val oSrcDef_0040 = SourceDefinition( oPathStructSrc_0040 )
val oSrcDef_0041 = SourceDefinition( oPathStructSrc_0041 )
val oSrcDef_0042 = SourceDefinition( oPathStructSrc_0042 )
val oSrcDef_0043 = SourceDefinition( oPathStructSrc_0043 )
val oSrcDef_0044 = SourceDefinition( oPathStructSrc_0044 )
val oSrcDef_0045 = SourceDefinition( oPathStructSrc_0045 )
val oSrcDef_0046 = SourceDefinition( oPathStructSrc_0046 )
val oSrcDef_0047 = SourceDefinition( oPathStructSrc_0047 )
val oSrcDef_0048 = SourceDefinition( oPathStructSrc_0048 )
val oSrcDef_0049 = SourceDefinition( oPathStructSrc_0049 )
val oSrcDef_0050 = SourceDefinition( oPathStructSrc_0050 )
val oSrcDef_0051 = SourceDefinition( oPathStructSrc_0051 )
val oSrcDef_0052 = SourceDefinition( oPathStructSrc_0052 )
val oSrcDef_0053 = SourceDefinition( oPathStructSrc_0053 )
val oSrcDef_0054 = SourceDefinition( oPathStructSrc_0054 )
//val oSrcDef_0055 = SourceDefinition( oPathStructSrc_0055 )
val oSrcDef_0056 = SourceDefinition( oPathStructSrc_0056 )
val oSrcDef_0057 = SourceDefinition( oPathStructSrc_0057 )
val oSrcDef_0058 = SourceDefinition( oPathStructSrc_0058 )
val oSrcDef_0059 = SourceDefinition( oPathStructSrc_0059 )
val oSrcDef_0060 = SourceDefinition( oPathStructSrc_0060 )
val oSrcDef_0061 = SourceDefinition( oPathStructSrc_0061 )
val oSrcDef_0062 = SourceDefinition( oPathStructSrc_0062 )
val oSrcDef_0063 = SourceDefinition( oPathStructSrc_0063 )
val oSrcDef_0064 = SourceDefinition( oPathStructSrc_0064 )
val oSrcDef_0065 = SourceDefinition( oPathStructSrc_0065 )
val oSrcDef_0066 = SourceDefinition( oPathStructSrc_0066 )
val oSrcDef_0067 = SourceDefinition( oPathStructSrc_0067 )
val oSrcDef_0068 = SourceDefinition( oPathStructSrc_0068 )
val oSrcDef_0069 = SourceDefinition( oPathStructSrc_0069 )
val oSrcDef_0070 = SourceDefinition( oPathStructSrc_0070 )
val oSrcDef_0071 = SourceDefinition( oPathStructSrc_0071 )
val oSrcDef_0072 = SourceDefinition( oPathStructSrc_0072 )
val oSrcDef_0073 = SourceDefinition( oPathStructSrc_0073 )
val oSrcDef_0074 = SourceDefinition( oPathStructSrc_0074 )
val oSrcDef_0075 = SourceDefinition( oPathStructSrc_0075 )
val oSrcDef_0076 = SourceDefinition( oPathStructSrc_0076 )
val oSrcDef_0077 = SourceDefinition( oPathStructSrc_0077 )
val oSrcDef_0078 = SourceDefinition( oPathStructSrc_0078 )
val oSrcDef_0079 = SourceDefinition( oPathStructSrc_0079 )
val oSrcDef_0080 = SourceDefinition( oPathStructSrc_0080 )
val oSrcDef_0081 = SourceDefinition( oPathStructSrc_0081 )
val oSrcDef_0082 = SourceDefinition( oPathStructSrc_0082 )
val oSrcDef_0083 = SourceDefinition( oPathStructSrc_0083 )
val oSrcDef_0084 = SourceDefinition( oPathStructSrc_0084 )
val oSrcDef_0085 = SourceDefinition( oPathStructSrc_0085 )
val oSrcDef_0086 = SourceDefinition( oPathStructSrc_0086 )
val oSrcDef_0087 = SourceDefinition( oPathStructSrc_0087 )
val oSrcDef_0088 = SourceDefinition( oPathStructSrc_0088 )
val oSrcDef_0089 = SourceDefinition( oPathStructSrc_0089 )
val oSrcDef_0090 = SourceDefinition( oPathStructSrc_0090 )
val oSrcDef_0091 = SourceDefinition( oPathStructSrc_0091 )
val oSrcDef_0092 = SourceDefinition( oPathStructSrc_0092 )
val oSrcDef_0093 = SourceDefinition( oPathStructSrc_0093 )
val oSrcDef_0094 = SourceDefinition( oPathStructSrc_0094 )
val oSrcDef_0095 = SourceDefinition( oPathStructSrc_0095 )
val oSrcDef_0096 = SourceDefinition( oPathStructSrc_0096 )
val oSrcDef_0097 = SourceDefinition( oPathStructSrc_0097 )
val oSrcDef_0098 = SourceDefinition( oPathStructSrc_0098 )
val oSrcDef_0099 = SourceDefinition( oPathStructSrc_0099 )
val oSrcDef_0100 = SourceDefinition( oPathStructSrc_0100 )
val oSrcDef_0101 = SourceDefinition( oPathStructSrc_0101 )
val oSrcDef_0102 = SourceDefinition( oPathStructSrc_0102 )
val oSrcDef_0103 = SourceDefinition( oPathStructSrc_0103 )
val oSrcDef_0104 = SourceDefinition( oPathStructSrc_0104 )
val oSrcDef_0105 = SourceDefinition( oPathStructSrc_0105 )
val oSrcDef_0106 = SourceDefinition( oPathStructSrc_0106 )
val oSrcDef_0107 = SourceDefinition( oPathStructSrc_0107 )
val oSrcDef_0108 = SourceDefinition( oPathStructSrc_0108 )
val oSrcDef_0109 = SourceDefinition( oPathStructSrc_0109 )
val oSrcDef_0110 = SourceDefinition( oPathStructSrc_0110 )
val oSrcDef_0111 = SourceDefinition( oPathStructSrc_0111 )
val oSrcDef_0112 = SourceDefinition( oPathStructSrc_0112 )
val oSrcDef_0113 = SourceDefinition( oPathStructSrc_0113 )
val oSrcDef_0114 = SourceDefinition( oPathStructSrc_0114 )
val oSrcDef_0115 = SourceDefinition( oPathStructSrc_0115 )
val oSrcDef_0116 = SourceDefinition( oPathStructSrc_0116 )
val oSrcDef_0117 = SourceDefinition( oPathStructSrc_0117 )
val oSrcDef_0118 = SourceDefinition( oPathStructSrc_0118 )
val oSrcDef_0119 = SourceDefinition( oPathStructSrc_0119 )
val oSrcDef_0120 = SourceDefinition( oPathStructSrc_0120 )
val oSrcDef_0121 = SourceDefinition( oPathStructSrc_0121 )
val oSrcDef_0122 = SourceDefinition( oPathStructSrc_0122 )
val oSrcDef_0123 = SourceDefinition( oPathStructSrc_0123 )
val oSrcDef_0124 = SourceDefinition( oPathStructSrc_0124 )
val oSrcDef_0125 = SourceDefinition( oPathStructSrc_0125 )
val oSrcDef_0126 = SourceDefinition( oPathStructSrc_0126 )
val oSrcDef_0127 = SourceDefinition( oPathStructSrc_0127 )
val oSrcDef_0128 = SourceDefinition( oPathStructSrc_0128 )
val oSrcDef_0129 = SourceDefinition( oPathStructSrc_0129 )
val oSrcDef_0130 = SourceDefinition( oPathStructSrc_0130 )
val oSrcDef_0131 = SourceDefinition( oPathStructSrc_0131 )
val oSrcDef_0132 = SourceDefinition( oPathStructSrc_0132 )
val oSrcDef_0133 = SourceDefinition( oPathStructSrc_0133 )
val oSrcDef_0134 = SourceDefinition( oPathStructSrc_0134 )
val oSrcDef_0135 = SourceDefinition( oPathStructSrc_0135 )
val oSrcDef_0136 = SourceDefinition( oPathStructSrc_0136 )
val oSrcDef_0137 = SourceDefinition( oPathStructSrc_0137 )
val oSrcDef_0138 = SourceDefinition( oPathStructSrc_0138 )
val oSrcDef_0139 = SourceDefinition( oPathStructSrc_0139 )
val oSrcDef_0140 = SourceDefinition( oPathStructSrc_0140 )
val oSrcDef_0141 = SourceDefinition( oPathStructSrc_0141 )
val oSrcDef_0142 = SourceDefinition( oPathStructSrc_0142 )
val oSrcDef_0143 = SourceDefinition( oPathStructSrc_0143 )
val oSrcDef_0144 = SourceDefinition( oPathStructSrc_0144 )
val oSrcDef_0145 = SourceDefinition( oPathStructSrc_0145 )
val oSrcDef_0146 = SourceDefinition( oPathStructSrc_0146 )

val oSrcDef_0501 = SourceDefinition( oPathStructSrc_0501 )
val oSrcDef_0502 = SourceDefinition( oPathStructSrc_0502 )
val oSrcDef_0503 = SourceDefinition( oPathStructSrc_0503 )
val oSrcDef_0504 = SourceDefinition( oPathStructSrc_0504 )
val oSrcDef_0505 = SourceDefinition( oPathStructSrc_0505 )
val oSrcDef_0506 = SourceDefinition( oPathStructSrc_0506 )
val oSrcDef_0507 = SourceDefinition( oPathStructSrc_0507 )
val oSrcDef_0508 = SourceDefinition( oPathStructSrc_0508 )


val oColumnTransformationDefinition_0001 = ColumnTransformationDefinition( oPathStructSrc_0001, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0002 = ColumnTransformationDefinition( oPathStructSrc_0002, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0003 = ColumnTransformationDefinition( oPathStructSrc_0003, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0004 = ColumnTransformationDefinition( oPathStructSrc_0004, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0005 = ColumnTransformationDefinition( oPathStructSrc_0005, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0006 = ColumnTransformationDefinition( oPathStructSrc_0006, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0007 = ColumnTransformationDefinition( oPathStructSrc_0007, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0008 = ColumnTransformationDefinition( oPathStructSrc_0008, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0009 = ColumnTransformationDefinition( oPathStructSrc_0009, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0010 = ColumnTransformationDefinition( oPathStructSrc_0010, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0011 = ColumnTransformationDefinition( oPathStructSrc_0011, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0012 = ColumnTransformationDefinition( oPathStructSrc_0012, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0013 = ColumnTransformationDefinition( oPathStructSrc_0013, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0014 = ColumnTransformationDefinition( oPathStructSrc_0014, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0015 = ColumnTransformationDefinition( oPathStructSrc_0015, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0016 = ColumnTransformationDefinition( oPathStructSrc_0016, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0017 = ColumnTransformationDefinition( oPathStructSrc_0017, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0018 = ColumnTransformationDefinition( oPathStructSrc_0018, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0019 = ColumnTransformationDefinition( oPathStructSrc_0019, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0020 = ColumnTransformationDefinition( oPathStructSrc_0020, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0021 = ColumnTransformationDefinition( oPathStructSrc_0021, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0022 = ColumnTransformationDefinition( oPathStructSrc_0022, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0023 = ColumnTransformationDefinition( oPathStructSrc_0023, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0024 = ColumnTransformationDefinition( oPathStructSrc_0024, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0025 = ColumnTransformationDefinition( oPathStructSrc_0025, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0026 = ColumnTransformationDefinition( oPathStructSrc_0026, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0027 = ColumnTransformationDefinition( oPathStructSrc_0027, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0028 = ColumnTransformationDefinition( oPathStructSrc_0028, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0029 = ColumnTransformationDefinition( oPathStructSrc_0029, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0030 = ColumnTransformationDefinition( oPathStructSrc_0030, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0031 = ColumnTransformationDefinition( oPathStructSrc_0031, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0032 = ColumnTransformationDefinition( oPathStructSrc_0032, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0033 = ColumnTransformationDefinition( oPathStructSrc_0033, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0034 = ColumnTransformationDefinition( oPathStructSrc_0034, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0035 = ColumnTransformationDefinition( oPathStructSrc_0035, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0036 = ColumnTransformationDefinition( oPathStructSrc_0036, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0037 = ColumnTransformationDefinition( oPathStructSrc_0037, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0038 = ColumnTransformationDefinition( oPathStructSrc_0038, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0039 = ColumnTransformationDefinition( oPathStructSrc_0039, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0040 = ColumnTransformationDefinition( oPathStructSrc_0040, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0041 = ColumnTransformationDefinition( oPathStructSrc_0041, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0042 = ColumnTransformationDefinition( oPathStructSrc_0042, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0043 = ColumnTransformationDefinition( oPathStructSrc_0043, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0044 = ColumnTransformationDefinition( oPathStructSrc_0044, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0045 = ColumnTransformationDefinition( oPathStructSrc_0045, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0046 = ColumnTransformationDefinition( oPathStructSrc_0046, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0047 = ColumnTransformationDefinition( oPathStructSrc_0047, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0048 = ColumnTransformationDefinition( oPathStructSrc_0048, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0049 = ColumnTransformationDefinition( oPathStructSrc_0049, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0050 = ColumnTransformationDefinition( oPathStructSrc_0050, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0051 = ColumnTransformationDefinition( oPathStructSrc_0051, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0052 = ColumnTransformationDefinition( oPathStructSrc_0052, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0053 = ColumnTransformationDefinition( oPathStructSrc_0053, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0054 = ColumnTransformationDefinition( oPathStructSrc_0054, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
//val oColumnTransformationDefinition_0055 = ColumnTransformationDefinition( oPathStructSrc_0055, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0056 = ColumnTransformationDefinition( oPathStructSrc_0056, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0057 = ColumnTransformationDefinition( oPathStructSrc_0057, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0058 = ColumnTransformationDefinition( oPathStructSrc_0058, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0059 = ColumnTransformationDefinition( oPathStructSrc_0059, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0060 = ColumnTransformationDefinition( oPathStructSrc_0060, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0061 = ColumnTransformationDefinition( oPathStructSrc_0061, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0062 = ColumnTransformationDefinition( oPathStructSrc_0062, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0063 = ColumnTransformationDefinition( oPathStructSrc_0063, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0064 = ColumnTransformationDefinition( oPathStructSrc_0064, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0065 = ColumnTransformationDefinition( oPathStructSrc_0065, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0066 = ColumnTransformationDefinition( oPathStructSrc_0066, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0067 = ColumnTransformationDefinition( oPathStructSrc_0067, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0068 = ColumnTransformationDefinition( oPathStructSrc_0068, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0069 = ColumnTransformationDefinition( oPathStructSrc_0069, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0070 = ColumnTransformationDefinition( oPathStructSrc_0070, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0071 = ColumnTransformationDefinition( oPathStructSrc_0071, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0072 = ColumnTransformationDefinition( oPathStructSrc_0072, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0073 = ColumnTransformationDefinition( oPathStructSrc_0073, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0074 = ColumnTransformationDefinition( oPathStructSrc_0074, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0075 = ColumnTransformationDefinition( oPathStructSrc_0075, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0076 = ColumnTransformationDefinition( oPathStructSrc_0076, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0077 = ColumnTransformationDefinition( oPathStructSrc_0077, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0078 = ColumnTransformationDefinition( oPathStructSrc_0078, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0079 = ColumnTransformationDefinition( oPathStructSrc_0079, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0080 = ColumnTransformationDefinition( oPathStructSrc_0080, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0081 = ColumnTransformationDefinition( oPathStructSrc_0081, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0082 = ColumnTransformationDefinition( oPathStructSrc_0082, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0083 = ColumnTransformationDefinition( oPathStructSrc_0083, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0084 = ColumnTransformationDefinition( oPathStructSrc_0084, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0085 = ColumnTransformationDefinition( oPathStructSrc_0085, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0086 = ColumnTransformationDefinition( oPathStructSrc_0086, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0087 = ColumnTransformationDefinition( oPathStructSrc_0087, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0088 = ColumnTransformationDefinition( oPathStructSrc_0088, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0089 = ColumnTransformationDefinition( oPathStructSrc_0089, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0090 = ColumnTransformationDefinition( oPathStructSrc_0090, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0091 = ColumnTransformationDefinition( oPathStructSrc_0091, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0092 = ColumnTransformationDefinition( oPathStructSrc_0092, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0093 = ColumnTransformationDefinition( oPathStructSrc_0093, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0094 = ColumnTransformationDefinition( oPathStructSrc_0094, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0095 = ColumnTransformationDefinition( oPathStructSrc_0095, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0096 = ColumnTransformationDefinition( oPathStructSrc_0096, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0097 = ColumnTransformationDefinition( oPathStructSrc_0097, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0098 = ColumnTransformationDefinition( oPathStructSrc_0098, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0099 = ColumnTransformationDefinition( oPathStructSrc_0099, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0100 = ColumnTransformationDefinition( oPathStructSrc_0100, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0101 = ColumnTransformationDefinition( oPathStructSrc_0101, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0102 = ColumnTransformationDefinition( oPathStructSrc_0102, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0103 = ColumnTransformationDefinition( oPathStructSrc_0103, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0104 = ColumnTransformationDefinition( oPathStructSrc_0104, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0105 = ColumnTransformationDefinition( oPathStructSrc_0105, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0106 = ColumnTransformationDefinition( oPathStructSrc_0106, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0107 = ColumnTransformationDefinition( oPathStructSrc_0107, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0108 = ColumnTransformationDefinition( oPathStructSrc_0108, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0109 = ColumnTransformationDefinition( oPathStructSrc_0109, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0110 = ColumnTransformationDefinition( oPathStructSrc_0110, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0111 = ColumnTransformationDefinition( oPathStructSrc_0111, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0112 = ColumnTransformationDefinition( oPathStructSrc_0112, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0113 = ColumnTransformationDefinition( oPathStructSrc_0113, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0114 = ColumnTransformationDefinition( oPathStructSrc_0114, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0115 = ColumnTransformationDefinition( oPathStructSrc_0115, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0116 = ColumnTransformationDefinition( oPathStructSrc_0116, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0117 = ColumnTransformationDefinition( oPathStructSrc_0117, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0118 = ColumnTransformationDefinition( oPathStructSrc_0118, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0119 = ColumnTransformationDefinition( oPathStructSrc_0119, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0120 = ColumnTransformationDefinition( oPathStructSrc_0120, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0121 = ColumnTransformationDefinition( oPathStructSrc_0121, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0122 = ColumnTransformationDefinition( oPathStructSrc_0122, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0123 = ColumnTransformationDefinition( oPathStructSrc_0123, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0124 = ColumnTransformationDefinition( oPathStructSrc_0124, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0125 = ColumnTransformationDefinition( oPathStructSrc_0125, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0126 = ColumnTransformationDefinition( oPathStructSrc_0126, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0127 = ColumnTransformationDefinition( oPathStructSrc_0127, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0128 = ColumnTransformationDefinition( oPathStructSrc_0128, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0129 = ColumnTransformationDefinition( oPathStructSrc_0129, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0130 = ColumnTransformationDefinition( oPathStructSrc_0130, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0131 = ColumnTransformationDefinition( oPathStructSrc_0131, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0132 = ColumnTransformationDefinition( oPathStructSrc_0132, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0133 = ColumnTransformationDefinition( oPathStructSrc_0133, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0134 = ColumnTransformationDefinition( oPathStructSrc_0134, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0135 = ColumnTransformationDefinition( oPathStructSrc_0135, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0136 = ColumnTransformationDefinition( oPathStructSrc_0136, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0137 = ColumnTransformationDefinition( oPathStructSrc_0137, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0138 = ColumnTransformationDefinition( oPathStructSrc_0138, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0139 = ColumnTransformationDefinition( oPathStructSrc_0139, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0140 = ColumnTransformationDefinition( oPathStructSrc_0140, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0141 = ColumnTransformationDefinition( oPathStructSrc_0141, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0142 = ColumnTransformationDefinition( oPathStructSrc_0142, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0143 = ColumnTransformationDefinition( oPathStructSrc_0143, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0144 = ColumnTransformationDefinition( oPathStructSrc_0144, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0145 = ColumnTransformationDefinition( oPathStructSrc_0145, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )
val oColumnTransformationDefinition_0146 = ColumnTransformationDefinition( oPathStructSrc_0146, "addStringStructFieldInArray", Seq( oSrcDef_0000, oSrcDef_1000 ), true )

val oColumnTransformationDefinition_0501 = ColumnTransformationDefinition( oPathStructSrc_0501, "collect",                     Seq( oSrcDef_0501 ),               true )
val oColumnTransformationDefinition_0502 = ColumnTransformationDefinition( oPathStructSrc_0502, "collect",                     Seq( oSrcDef_0502 ),               true )
val oColumnTransformationDefinition_0503 = ColumnTransformationDefinition( oPathStructSrc_0503, "collect",                     Seq( oSrcDef_0503 ),               true )
val oColumnTransformationDefinition_0504 = ColumnTransformationDefinition( oPathStructSrc_0504, "collect",                     Seq( oSrcDef_0504 ),               true )
val oColumnTransformationDefinition_0505 = ColumnTransformationDefinition( oPathStructSrc_0505, "collect",                     Seq( oSrcDef_0505 ),               true )
val oColumnTransformationDefinition_0506 = ColumnTransformationDefinition( oPathStructSrc_0506, "collect",                     Seq( oSrcDef_0506 ),               true )
val oColumnTransformationDefinition_0507 = ColumnTransformationDefinition( oPathStructSrc_0507, "collect",                     Seq( oSrcDef_0507 ),               true )
val oColumnTransformationDefinition_0508 = ColumnTransformationDefinition( oPathStructSrc_0508, "collect",                     Seq( oSrcDef_0508 ),               true )


  
  
  
  
  def getFullTransformation(): TransformationDefinition = 
  {
    val oTransformationDefinition_0001 = TransformationDefinition( Seq(
    oColumnTransformationDefinition_0001,
    oColumnTransformationDefinition_0002,
    oColumnTransformationDefinition_0003,
    oColumnTransformationDefinition_0004,
    oColumnTransformationDefinition_0005,
    oColumnTransformationDefinition_0006,
    oColumnTransformationDefinition_0007,
    oColumnTransformationDefinition_0008,
    oColumnTransformationDefinition_0009,
    oColumnTransformationDefinition_0010,
    oColumnTransformationDefinition_0011,
    oColumnTransformationDefinition_0012,
    oColumnTransformationDefinition_0013,
    oColumnTransformationDefinition_0014,
    oColumnTransformationDefinition_0015,
    oColumnTransformationDefinition_0016,
    oColumnTransformationDefinition_0017,
    oColumnTransformationDefinition_0018,
    oColumnTransformationDefinition_0019,
    oColumnTransformationDefinition_0020,
    oColumnTransformationDefinition_0021,
    oColumnTransformationDefinition_0022,
    oColumnTransformationDefinition_0023,
    oColumnTransformationDefinition_0024,
    oColumnTransformationDefinition_0025,
    oColumnTransformationDefinition_0026,
    oColumnTransformationDefinition_0027,
    oColumnTransformationDefinition_0028,
    oColumnTransformationDefinition_0029,
    oColumnTransformationDefinition_0030,
    oColumnTransformationDefinition_0031,
    oColumnTransformationDefinition_0032,
    oColumnTransformationDefinition_0033,
    oColumnTransformationDefinition_0034,
    oColumnTransformationDefinition_0035,
    oColumnTransformationDefinition_0036,
    oColumnTransformationDefinition_0037,
    oColumnTransformationDefinition_0038,
    oColumnTransformationDefinition_0039,
    oColumnTransformationDefinition_0040,
    oColumnTransformationDefinition_0041,
    oColumnTransformationDefinition_0042,
    oColumnTransformationDefinition_0043,
    oColumnTransformationDefinition_0044,
    oColumnTransformationDefinition_0045,
    oColumnTransformationDefinition_0046,
    oColumnTransformationDefinition_0047,
    oColumnTransformationDefinition_0048,
    oColumnTransformationDefinition_0049,
    oColumnTransformationDefinition_0050,
    oColumnTransformationDefinition_0051,
    oColumnTransformationDefinition_0052,
    oColumnTransformationDefinition_0053,
    oColumnTransformationDefinition_0054,
//    oColumnTransformationDefinition_0055,
    oColumnTransformationDefinition_0056,
    oColumnTransformationDefinition_0057,
    oColumnTransformationDefinition_0058,
    oColumnTransformationDefinition_0059,
    oColumnTransformationDefinition_0060,
    oColumnTransformationDefinition_0061,
    oColumnTransformationDefinition_0062,
    oColumnTransformationDefinition_0063,
    oColumnTransformationDefinition_0064,
    oColumnTransformationDefinition_0065,
    oColumnTransformationDefinition_0066,
    oColumnTransformationDefinition_0067,
    oColumnTransformationDefinition_0068,
    oColumnTransformationDefinition_0069,
    oColumnTransformationDefinition_0070,
    oColumnTransformationDefinition_0071,
    oColumnTransformationDefinition_0072,
    oColumnTransformationDefinition_0073,
    oColumnTransformationDefinition_0074,
    oColumnTransformationDefinition_0075,
    oColumnTransformationDefinition_0076,
    oColumnTransformationDefinition_0077,
    oColumnTransformationDefinition_0078,
    oColumnTransformationDefinition_0079,
    oColumnTransformationDefinition_0080,
    oColumnTransformationDefinition_0081,
    oColumnTransformationDefinition_0082,
    oColumnTransformationDefinition_0083,
    oColumnTransformationDefinition_0084,
    oColumnTransformationDefinition_0085,
    oColumnTransformationDefinition_0086,
    oColumnTransformationDefinition_0087,
    oColumnTransformationDefinition_0088,
    oColumnTransformationDefinition_0089,
    oColumnTransformationDefinition_0090,
    oColumnTransformationDefinition_0091,
    oColumnTransformationDefinition_0092,
    oColumnTransformationDefinition_0093,
    oColumnTransformationDefinition_0094,
    oColumnTransformationDefinition_0095,
    oColumnTransformationDefinition_0096,
    oColumnTransformationDefinition_0097,
    oColumnTransformationDefinition_0098,
    oColumnTransformationDefinition_0099,
    oColumnTransformationDefinition_0100,
    oColumnTransformationDefinition_0101,
    oColumnTransformationDefinition_0102,
    oColumnTransformationDefinition_0103,
    oColumnTransformationDefinition_0104,
    oColumnTransformationDefinition_0105,
    oColumnTransformationDefinition_0106,
    oColumnTransformationDefinition_0107,
    oColumnTransformationDefinition_0108,
    oColumnTransformationDefinition_0109,
    oColumnTransformationDefinition_0110,
    oColumnTransformationDefinition_0111,
    oColumnTransformationDefinition_0112,
    oColumnTransformationDefinition_0113,
    oColumnTransformationDefinition_0114,
    oColumnTransformationDefinition_0115,
    oColumnTransformationDefinition_0116,
    oColumnTransformationDefinition_0117,
    oColumnTransformationDefinition_0118,
    oColumnTransformationDefinition_0119,
    oColumnTransformationDefinition_0120,
    oColumnTransformationDefinition_0121,
    oColumnTransformationDefinition_0122,
    oColumnTransformationDefinition_0123,
    oColumnTransformationDefinition_0124,
    oColumnTransformationDefinition_0125,
    oColumnTransformationDefinition_0126,
    oColumnTransformationDefinition_0127,
    oColumnTransformationDefinition_0128,
    oColumnTransformationDefinition_0129,
    oColumnTransformationDefinition_0130,
    oColumnTransformationDefinition_0131,
    oColumnTransformationDefinition_0132,
    oColumnTransformationDefinition_0133,
    oColumnTransformationDefinition_0134,
    oColumnTransformationDefinition_0135,
    oColumnTransformationDefinition_0136,
    oColumnTransformationDefinition_0137,
    oColumnTransformationDefinition_0138,
    oColumnTransformationDefinition_0139,
    oColumnTransformationDefinition_0140,
    oColumnTransformationDefinition_0141,
    oColumnTransformationDefinition_0142,
    oColumnTransformationDefinition_0143,
    oColumnTransformationDefinition_0144,
    oColumnTransformationDefinition_0145,
    oColumnTransformationDefinition_0146,
    oColumnTransformationDefinition_0501,
    oColumnTransformationDefinition_0502,
    oColumnTransformationDefinition_0503,
    oColumnTransformationDefinition_0504,
    oColumnTransformationDefinition_0505,
    oColumnTransformationDefinition_0506,
    oColumnTransformationDefinition_0507,
    oColumnTransformationDefinition_0508
    
    ))
    
    oTransformationDefinition_0001
  }
  
  
  def getObjectIdTransformationOnly(): TransformationDefinition = 
  {
        val oTransformationDefinition_0001 = TransformationDefinition( Seq(
    oColumnTransformationDefinition_0001,
    oColumnTransformationDefinition_0002,
    oColumnTransformationDefinition_0003,
    oColumnTransformationDefinition_0004,
    oColumnTransformationDefinition_0005,
    oColumnTransformationDefinition_0006,
    oColumnTransformationDefinition_0007,
    oColumnTransformationDefinition_0008,
    oColumnTransformationDefinition_0009,
    oColumnTransformationDefinition_0010,
    oColumnTransformationDefinition_0011,
    oColumnTransformationDefinition_0012,
    oColumnTransformationDefinition_0013,
    oColumnTransformationDefinition_0014,
    oColumnTransformationDefinition_0015,
    oColumnTransformationDefinition_0016,
    oColumnTransformationDefinition_0017,
    oColumnTransformationDefinition_0018,
    oColumnTransformationDefinition_0019,
    oColumnTransformationDefinition_0020,
    oColumnTransformationDefinition_0021,
    oColumnTransformationDefinition_0022,
    oColumnTransformationDefinition_0023,
    oColumnTransformationDefinition_0024,
    oColumnTransformationDefinition_0025,
    oColumnTransformationDefinition_0026,
    oColumnTransformationDefinition_0027,
    oColumnTransformationDefinition_0028,
    oColumnTransformationDefinition_0029,
    oColumnTransformationDefinition_0030,
    oColumnTransformationDefinition_0031,
    oColumnTransformationDefinition_0032,
    oColumnTransformationDefinition_0033,
    oColumnTransformationDefinition_0034,
    oColumnTransformationDefinition_0035,
    oColumnTransformationDefinition_0036,
    oColumnTransformationDefinition_0037,
    oColumnTransformationDefinition_0038,
    oColumnTransformationDefinition_0039,
    oColumnTransformationDefinition_0040,
    oColumnTransformationDefinition_0041,
    oColumnTransformationDefinition_0042,
    oColumnTransformationDefinition_0043,
    oColumnTransformationDefinition_0044,
    oColumnTransformationDefinition_0045,
    oColumnTransformationDefinition_0046,
    oColumnTransformationDefinition_0047,
    oColumnTransformationDefinition_0048,
    oColumnTransformationDefinition_0049,
    oColumnTransformationDefinition_0050,
    oColumnTransformationDefinition_0051,
    oColumnTransformationDefinition_0052,
    oColumnTransformationDefinition_0053,
    oColumnTransformationDefinition_0054,
//    oColumnTransformationDefinition_0055,
    oColumnTransformationDefinition_0056,
    oColumnTransformationDefinition_0057,
    oColumnTransformationDefinition_0058,
    oColumnTransformationDefinition_0059,
    oColumnTransformationDefinition_0060,
    oColumnTransformationDefinition_0061,
    oColumnTransformationDefinition_0062,
    oColumnTransformationDefinition_0063,
    oColumnTransformationDefinition_0064,
    oColumnTransformationDefinition_0065,
    oColumnTransformationDefinition_0066,
    oColumnTransformationDefinition_0067,
    oColumnTransformationDefinition_0068,
    oColumnTransformationDefinition_0069,
    oColumnTransformationDefinition_0070,
    oColumnTransformationDefinition_0071,
    oColumnTransformationDefinition_0072,
    oColumnTransformationDefinition_0073,
    oColumnTransformationDefinition_0074,
    oColumnTransformationDefinition_0075,
    oColumnTransformationDefinition_0076,
    oColumnTransformationDefinition_0077,
    oColumnTransformationDefinition_0078,
    oColumnTransformationDefinition_0079,
    oColumnTransformationDefinition_0080,
    oColumnTransformationDefinition_0081,
    oColumnTransformationDefinition_0082,
    oColumnTransformationDefinition_0083,
    oColumnTransformationDefinition_0084,
    oColumnTransformationDefinition_0085,
    oColumnTransformationDefinition_0086,
    oColumnTransformationDefinition_0087,
    oColumnTransformationDefinition_0088,
    oColumnTransformationDefinition_0089,
    oColumnTransformationDefinition_0090,
    oColumnTransformationDefinition_0091,
    oColumnTransformationDefinition_0092,
    oColumnTransformationDefinition_0093,
    oColumnTransformationDefinition_0094,
    oColumnTransformationDefinition_0095,
    oColumnTransformationDefinition_0096,
    oColumnTransformationDefinition_0097,
    oColumnTransformationDefinition_0098,
    oColumnTransformationDefinition_0099,
    oColumnTransformationDefinition_0100,
    oColumnTransformationDefinition_0101,
    oColumnTransformationDefinition_0102,
    oColumnTransformationDefinition_0103,
    oColumnTransformationDefinition_0104,
    oColumnTransformationDefinition_0105,
    oColumnTransformationDefinition_0106,
    oColumnTransformationDefinition_0107,
    oColumnTransformationDefinition_0108,
    oColumnTransformationDefinition_0109,
    oColumnTransformationDefinition_0110,
    oColumnTransformationDefinition_0111,
    oColumnTransformationDefinition_0112,
    oColumnTransformationDefinition_0113,
    oColumnTransformationDefinition_0114,
    oColumnTransformationDefinition_0115,
    oColumnTransformationDefinition_0116,
    oColumnTransformationDefinition_0117,
    oColumnTransformationDefinition_0118,
    oColumnTransformationDefinition_0119,
    oColumnTransformationDefinition_0120,
    oColumnTransformationDefinition_0121,
    oColumnTransformationDefinition_0122,
    oColumnTransformationDefinition_0123,
    oColumnTransformationDefinition_0124,
    oColumnTransformationDefinition_0125,
    oColumnTransformationDefinition_0126,
    oColumnTransformationDefinition_0127,
    oColumnTransformationDefinition_0128,
    oColumnTransformationDefinition_0129,
    oColumnTransformationDefinition_0130,
    oColumnTransformationDefinition_0131,
    oColumnTransformationDefinition_0132,
    oColumnTransformationDefinition_0133,
    oColumnTransformationDefinition_0134,
    oColumnTransformationDefinition_0135,
    oColumnTransformationDefinition_0136,
    oColumnTransformationDefinition_0137,
    oColumnTransformationDefinition_0138,
    oColumnTransformationDefinition_0139,
    oColumnTransformationDefinition_0140,
    oColumnTransformationDefinition_0141,
    oColumnTransformationDefinition_0142,
    oColumnTransformationDefinition_0143,
    oColumnTransformationDefinition_0144,
    oColumnTransformationDefinition_0145,
    oColumnTransformationDefinition_0146
    
    ))
    
    oTransformationDefinition_0001
  }
  
  def getArrayTransformationOnly(): TransformationDefinition = 
  {
    val oTransformationDefinition_0001 = TransformationDefinition( Seq(
    oColumnTransformationDefinition_0501,
    oColumnTransformationDefinition_0502,
    oColumnTransformationDefinition_0503,
    oColumnTransformationDefinition_0504,
    oColumnTransformationDefinition_0505,
    oColumnTransformationDefinition_0506,
    oColumnTransformationDefinition_0507,
    oColumnTransformationDefinition_0508
    ))
    
    oTransformationDefinition_0001
  }
  
  
  
}