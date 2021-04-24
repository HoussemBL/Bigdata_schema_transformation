/**
 * JavaScript snippert to validate version and SCM information and to construct
 * a revision from it.
 * 
 * @param plainVersion
 *          The plain version from the POM, e.g. '2.1.0'
 * @param scmBranch
 *          The SCM branch, e.g. 'develop', 'feature-1234-xy', 'master', ...
 * @param scmTags
 *          The SCM tags for the current revision, e.g. '2.1.0-rc032,2.1.0',
 *          ....
 * @return property 'revision.value' with the actual revision on success,
 *         property 'revision.error' on failure.
 */


function isTag( strBranch ){
	
	return branch.startsWith('refs/tags/');
}


var plainVersion = project.getProperty('plainVersion');
var branch       = project.getProperty('scmBranch');
var tags         = project.getProperty('scmTags');
var date         = project.getProperty('date.now');
var revision     = null;
var error        = null;

print("\nbuild-revision.js: Start" );
print("\nbuild-revision.js: plainVersion: [" + plainVersion + "]" );
print("\nbuild-revision.js: branch: ......[" + branch       + "]" );
print("\nbuild-revision.js: tags: ........[" + tags         + "]" );
print("\nbuild-revision.js: date: ........[" + date         + "]" );



if ( isTag(branch) ){
	revision = tags;
}//-- if ( isTag(branch) )
else{
	revision = branch + "-" + date
}


if (error !== null) {
  project.setProperty('revision.error', error);
} else if (revision !== null) {
  project.setProperty('revision.value', revision);
} else {
  project.setProperty('revision.error', 'Could not determine revision');
}

print("\nbuild-revision.js: End" );


