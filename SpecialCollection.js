const { request } = require('undici'); // Use undici for faster HTTP requests
// const m3u8Parser = require('m3u8-parser'); // m3u8-parser no se usa directamente para la lógica principal de parseo de #EXTINF
const fs = require('fs').promises;
const path = require('path');
const yaml = require('js-yaml');

// Load p-limit dynamically since it's an ES Module
let pLimit;
(async () => {
    pLimit = (await import('p-limit')).default;
})();

// List of common country names for detection
const countries = [
    'USA', 'India', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Italy', 'Spain', 'Brazil',
    'China', 'Japan', 'Korea', 'Mexico', 'Russia', 'South Africa', 'Argentina', 'Netherlands', 'Sweden', 'Norway',
    'Turkey', 'Poland', 'Belgium', 'Switzerland', 'Austria', 'Portugal', 'Denmark', 'Finland', 'Ireland',
    'New Zealand', 'South Korea', 'UAE', 'Saudi Arabia', 'Qatar', 'Egypt', 'Nigeria', 'Colombia', 'Chile',
    'Peru', 'Thailand', 'Vietnam', 'Malaysia', 'Indonesia', 'Philippines', 'Pakistan', 'Bangladesh',
    'DE', 'TR', 'NL', 'FR', 'GB', 'US', 'IT', 'ES', 'PT', 'BE', 'CH', 'AT', 'PL', 'DK', 'SE', 'NO', 'FI',
    'GR', 'CY', 'CZ', 'HU', 'RO', 'BG', 'RS', 'HR', 'BA', 'ME', 'MK', 'AL', 'RU', 'UA', 'BY', 'KZ'
];

// URLs for M3U playlist lists from GitHub
const GITHUB_URL_SOURCES = [
    'https://raw.githubusercontent.com/patr0nq/link/refs/heads/main/randomurl.txt'
];

// Load configuration from YAML file
async function loadConfig() {
    try {
        const configContent = await fs.readFile('config.yml', 'utf8');
        return yaml.load(configContent);
    } catch (error) {
        console.error('Error loading config.yml:', error.message);
        // Provide default settings if config.yml is missing or fails to load
        console.warn('Using default settings as config.yml could not be loaded.');
        return {
            settings: {
                concurrency: 10,
                fetchTimeout: 30000, // 30 seconds
                linkCheckTimeout: 15000, // 15 seconds
                batchSize: 200,
                outputDirPrefix: 'SpecialLinks',
                channelsPerFile: 5000
            },
            // urls: [] // urls will be populated from GitHub
        };
    }
}

// Function to fetch content from a remote URL list (like the GitHub raw files)
async function fetchRemoteUrlList(url, timeout) {
    console.log(`Fetching URL list from ${url}...`);
    try {
        const { body, statusCode } = await request(url, {
            method: 'GET',
            maxRedirections: 3, // Allow a few more redirections for raw GitHub content
            timeout
        });
        if (statusCode !== 200) {
            throw new Error(`Failed to fetch ${url}: Status ${statusCode}`);
        }
        return await body.text();
    } catch (error) {
        console.error(`Error fetching URL list ${url}:`, error.message);
        return null;
    }
}

// Function to parse the remote URL list content
function parseRemoteUrlList(content) {
    if (!content) return [];
    const urls = [];
    const lines = content.split('\n');
    const urlRegex = /^(https?:\/\/[^\s]+)/; // Regex to extract URL

    lines.forEach(line => {
        const trimmedLine = line.trim();
        if (trimmedLine.startsWith('#') || trimmedLine === '' || trimmedLine.includes('Real Url:')) {
            // Skip comments, empty lines, or lines with "Real Url:" that are not the primary URL
            return;
        }
        const match = trimmedLine.match(urlRegex);
        if (match) {
            const potentialUrl = match[1];
            // Add a basic filter to check if it's likely an M3U playlist URL
            if (potentialUrl.includes('get.php') || potentialUrl.includes('type=m3u') || potentialUrl.includes('output=ts') || potentialUrl.endsWith('.m3u') || potentialUrl.endsWith('.m3u8')) {
                urls.push(potentialUrl);
            }
        }
    });
    return [...new Set(urls)]; // Remove duplicates
}


// Function to fetch M3U content from a URL
async function fetchM3U(url, timeout) {
    try {
        const { body, statusCode } = await request(url, {
            method: 'GET',
            maxRedirections: 5, // Increased maxRedirections
            timeout,
            headers: { // Add common headers
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
                'Connection': 'keep-alive'
            }
        });
        if (statusCode !== 200) {
            if (statusCode === 403 || statusCode === 401) {
                 console.warn(`Access denied for ${url}: Status ${statusCode}. Skipping.`);
                 return null;
            }
            throw new Error(`Failed to fetch ${url}: Status ${statusCode}`);
        }
        return await body.text();
    } catch (error) {
        console.error(`Error fetching M3U ${url}:`, error.message.length > 200 ? error.message.substring(0,200) + "..." : error.message);
        return null;
    }
}

// Function to parse M3U content and extract channel links with groups
function parseM3U(content) {
    const channels = [];
    const lines = content.split('\n');
    let currentChannel = null;

    lines.forEach(line => {
        line = line.trim();
        if (line.startsWith('#EXTINF')) {
            const groupMatch = line.match(/group-title="([^"]+)"/);
            const nameMatch = line.match(/,(.+)/);
            const tvgIdMatch = line.match(/tvg-id="([^"]*)"/);
            const tvgLogoMatch = line.match(/tvg-logo="([^"]*)"/);
            const tvgNameMatch = line.match(/tvg-name="([^"]*)"/);


            currentChannel = {
                name: nameMatch ? nameMatch[1] : 'Unknown',
                url: null,
                group: groupMatch ? groupMatch[1] : 'Unknown',
                tvgId: tvgIdMatch ? tvgIdMatch[1] : '',
                tvgLogo: tvgLogoMatch ? tvgLogoMatch[1] : '',
                tvgName: tvgNameMatch ? tvgNameMatch[1] : (nameMatch ? nameMatch[1] : 'Unknown')
            };
        } else if (line && !line.startsWith('#') && currentChannel) {
            currentChannel.url = line;
            if (currentChannel.url.startsWith('http')) { // Basic validation for URL
                channels.push(currentChannel);
            } else {
                console.warn(`Skipping invalid URL for channel "${currentChannel.name}": ${currentChannel.url}`);
            }
            currentChannel = null;
        }
    });

    return channels.filter(ch => ch.url); // Only return channels with URLs
}

// Function to check if a batch of links is active
async function checkLinkBatch(urls, timeout, concurrency) {
    while (!pLimit) {
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    const limit = pLimit(concurrency);
    const results = await Promise.allSettled(urls.map(urlInfo => // urlInfo is now {url: string, name: string, group: string}
        limit(async () => {
            try {
                const { statusCode, headers } = await request(urlInfo.url, {
                    method: 'HEAD', // Use HEAD for faster check
                    maxRedirections: 3, // Allow some redirections
                    timeout,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': '*/*'
                    }
                });
                // Check for common video/streaming content types if HEAD is successful
                const contentType = headers['content-type'] || '';
                const isActive = (statusCode >= 200 && statusCode < 400) && 
                                 (contentType.includes('video') || 
                                  contentType.includes('mpegurl') || 
                                  contentType.includes('octet-stream') ||
                                  contentType.includes('application/vnd.apple.mpegurl') ||
                                  true); // Relaxed to allow more types if content-type is unhelpful

                return { ...urlInfo, isActive };
            } catch (error) {
                // console.warn(`Link check failed for ${urlInfo.url}: ${error.message}`);
                return { ...urlInfo, isActive: false };
            }
        })
    ));

    return results
        .filter(result => result.status === 'fulfilled')
        .map(result => result.value)
        .filter(result => result.isActive);
}


// Function to collect and process all M3U links
async function collectActiveLinks(m3uPlaylistUrls, concurrency, fetchTimeout, linkCheckTimeout, batchSize) {
    while (!pLimit) {
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    const limitFetch = pLimit(concurrency); // Limit for fetching M3U playlists
    const allChannelsMap = new Map(); // Use Map to avoid duplicates (keyed by URL)

    console.log(`Starting to fetch and parse ${m3uPlaylistUrls.length} M3U playlists...`);

    const fetchAndParsePromises = m3uPlaylistUrls.map(playlistUrl =>
        limitFetch(async () => {
            console.log(`Fetching M3U playlist from ${playlistUrl}...`);
            const m3uContent = await fetchM3U(playlistUrl, fetchTimeout);
            if (!m3uContent) {
                console.warn(`No content fetched from ${playlistUrl}. Skipping.`);
                return [];
            }

            // console.log(`Parsing M3U from ${playlistUrl}...`);
            const channels = parseM3U(m3uContent);
            // console.log(`Found ${channels.length} channels in ${playlistUrl}`);

            channels.forEach(channel => {
                if (channel.url && !allChannelsMap.has(channel.url)) { // Ensure URL exists
                    allChannelsMap.set(channel.url, channel);
                }
            });
            return channels.length; // Return count for logging
        })
    );

    const results = await Promise.allSettled(fetchAndParsePromises);
    results.forEach((result, index) => {
        if (result.status === 'fulfilled') {
            // console.log(`Successfully processed M3U: ${m3uPlaylistUrls[index]}, found ${result.value} channels initially.`);
        } else {
            console.error(`Failed to process M3U ${m3uPlaylistUrls[index]}: ${result.reason.message}`);
        }
    });

    const uniqueChannels = Array.from(allChannelsMap.values());
    console.log(`Collected ${uniqueChannels.length} unique channels from all playlists.`);

    if (uniqueChannels.length === 0) {
        console.log("No unique channels found to check.");
        return [];
    }
    
    const activeChannels = [];
    const limitCheck = pLimit(concurrency); // Limit for checking individual links

    console.log(`Checking activity for ${uniqueChannels.length} channels in batches of ${batchSize}...`);
    for (let i = 0; i < uniqueChannels.length; i += batchSize) {
        const batch = uniqueChannels.slice(i, i + batchSize);
        console.log(`Checking batch ${Math.floor(i / batchSize) + 1} / ${Math.ceil(uniqueChannels.length / batchSize)} (${batch.length} links)...`);
        
        const batchUrlsToInfo = batch.map(ch => ({ url: ch.url, name: ch.name, group: ch.group, tvgId: ch.tvgId, tvgLogo: ch.tvgLogo, tvgName: ch.tvgName }));

        const activeLinksInBatch = await checkLinkBatch(batchUrlsToInfo, linkCheckTimeout, concurrency);
        
        activeLinksInBatch.forEach(activeLinkInfo => {
            // Reconstruct channel object from activeLinkInfo
            const channel = allChannelsMap.get(activeLinkInfo.url);
            if (channel) {
                 activeChannels.push(channel); // Push the original full channel object
            }
        });
        console.log(`Processed batch ${Math.floor(i / batchSize) + 1}. Total active channels so far: ${activeChannels.length}`);
    }

    console.log(`Found ${activeChannels.length} active and unique channels in total.`);
    return activeChannels;
}


// Function to group channels by group-title and country (if applicable)
function groupChannels(channels) {
    const grouped = {};

    channels.forEach(channel => {
        let groupName = channel.group || 'Unknown';
        let countryName = 'Unknown';

        // Check if the group contains a country name or code
        const foundCountry = countries.find(c => 
            groupName.toLowerCase().includes(c.toLowerCase()) || 
            (channel.name && channel.name.toLowerCase().includes(c.toLowerCase())) || // Check channel name too
            (channel.tvgName && channel.tvgName.toLowerCase().includes(c.toLowerCase())) // Check tvg-name
        );

        if (foundCountry) {
            countryName = foundCountry.toUpperCase(); // Standardize to uppercase
            // Try to make group name more generic if country is found
            const countryRegex = new RegExp(`\\b${foundCountry}\\b`, 'i');
            let cleanedGroup = groupName.replace(countryRegex, '').trim();
            cleanedGroup = cleanedGroup.replace(/[|()\[\]]/g, '').replace(/\s+/g, ' ').trim(); // Remove special chars and extra spaces
            groupName = cleanedGroup || 'General';
        }
        
        // Further clean up group name
        groupName = groupName.replace(/[^a-zA-Z0-9\s_&-]/g, '').replace(/\s+/g, ' ').trim(); // Allow space, _, &, -
        if (groupName.length === 0 || groupName.toLowerCase() === 'unknown') groupName = 'General';


        const groupKey = groupName;
        if (!grouped[groupKey]) {
            grouped[groupKey] = {};
        }
        if (!grouped[groupKey][countryName]) {
            grouped[groupKey][countryName] = [];
        }
        grouped[groupKey][countryName].push(channel);
    });

    return grouped;
}

// Function to split channels into chunks of specified size
function splitChannels(channels, channelsPerFile) {
    const chunks = [];
    for (let i = 0; i < channels.length; i += channelsPerFile) {
        chunks.push(channels.slice(i, i + channelsPerFile));
    }
    return chunks;
}

// Function to delete a directory recursively
async function deleteDirectory(dirPath) {
    try {
        await fs.rm(dirPath, { recursive: true, force: true });
        console.log(`Deleted existing directory: ${dirPath}`);
    } catch (error) {
        if (error.code !== 'ENOENT') { // Ignore if directory doesn't exist
            console.error(`Error deleting directory ${dirPath}:`, error.message);
        }
    }
}

// Function to save active links to multiple formats with grouping and splitting
async function saveResults(channels, outputDirPrefix, channelsPerFile, concurrency) {
    while (!pLimit) {
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    const limit = pLimit(concurrency > 5 ? 5 : concurrency); // Reduce concurrency for file I/O

    await deleteDirectory(outputDirPrefix);
    console.log(`Ensuring base output directory exists: ${outputDirPrefix}`);
    await fs.mkdir(outputDirPrefix, { recursive: true });


    const groupedChannels = groupChannels(channels);
    const savePromises = [];

    for (const [group, countriesObj] of Object.entries(groupedChannels)) {
        for (const [country, countryChannels] of Object.entries(countriesObj)) {
            if (countryChannels.length === 0) {
                // console.log(`Skipping empty group: ${group}/${country}`);
                continue;
            }

            savePromises.push(limit(async () => {
                const safeGroup = group.replace(/[^a-zA-Z0-9_&-]/g, '_').replace(/\s+/g, '_');
                const safeCountry = country.replace(/[^a-zA-Z0-9_&-]/g, '_').replace(/\s+/g, '_');
                const groupDir = path.join(outputDirPrefix, safeGroup, safeCountry);

                await fs.mkdir(groupDir, { recursive: true });

                const channelChunks = splitChannels(countryChannels, channelsPerFile);

                for (let i = 0; i < channelChunks.length; i++) {
                    const chunk = channelChunks[i];
                    if (chunk.length === 0) {
                        continue;
                    }

                    const suffix = channelChunks.length > 1 ? (i + 1) : '';
                    const baseName = `SpecialLinks${suffix}`;
                    
                    let m3uContent = '#EXTM3U\n';
                    chunk.forEach(ch => {
                        let extinfLine = `#EXTINF:-1 tvg-id="${ch.tvgId || ''}" tvg-name="${ch.tvgName || ch.name}" tvg-logo="${ch.tvgLogo || ''}" group-title="${group}",${ch.name}\n${ch.url}\n`;
                        m3uContent += extinfLine;
                    });

                    const jsonContent = JSON.stringify(chunk.map(ch => ({ name: ch.name, url: ch.url, group: group, country: country, tvgId: ch.tvgId, tvgLogo: ch.tvgLogo, tvgName: ch.tvgName })), null, 2);
                    const txtContent = chunk.map(ch => ch.url).join('\n');

                    try {
                        await Promise.all([
                            fs.writeFile(path.join(groupDir, `${baseName}.m3u`), m3uContent),
                            fs.writeFile(path.join(groupDir, `${baseName}.json`), jsonContent),
                            fs.writeFile(path.join(groupDir, `${baseName}.txt`), txtContent)
                        ]);
                        console.log(`Saved ${chunk.length} active links to ${groupDir}/${baseName}.*`);
                    } catch (writeError) {
                        console.error(`Error writing files for ${groupDir}/${baseName}.*:`, writeError.message);
                    }
                }
            }));
        }
    }

    const results = await Promise.allSettled(savePromises);
    const successfulSaves = results.filter(result => result.status === 'fulfilled');
    if (successfulSaves.length === 0 && channels.length > 0) {
        console.log('No groups successfully saved, but active channels were present. This might indicate an issue with directory creation or grouping.');
    } else if (channels.length === 0) {
         console.log('No active channels found to save. Removing empty SpecialLinks directory if it was created.');
         await deleteDirectory(outputDirPrefix); // Clean up if no channels
    }
}

// Main function to run the script
async function main() {
    console.log('Starting M3U link collection...');

    const config = await loadConfig();
    // const { urls: configUrls, settings } = config; // configUrls might be undefined
    const { settings } = config;
    const { concurrency, fetchTimeout, linkCheckTimeout, batchSize, outputDirPrefix, channelsPerFile } = settings;

    let allRemoteM3uPlaylistUrls = [];

    console.log('Fetching M3U playlist URLs from GitHub sources...');
    for (const sourceUrl of GITHUB_URL_SOURCES) {
        const listContent = await fetchRemoteUrlList(sourceUrl, fetchTimeout);
        if (listContent) {
            const parsedUrls = parseRemoteUrlList(listContent);
            console.log(`Found ${parsedUrls.length} M3U playlist URLs in ${sourceUrl}`);
            allRemoteM3uPlaylistUrls.push(...parsedUrls);
        }
    }
    
    // Optionally, combine with URLs from config.yml if needed
    // if (configUrls && Array.isArray(configUrls)) {
    //     allRemoteM3uPlaylistUrls.push(...configUrls);
    // }

    // Remove duplicates from the combined list
    allRemoteM3uPlaylistUrls = [...new Set(allRemoteM3uPlaylistUrls)];

    if (allRemoteM3uPlaylistUrls.length === 0) {
        console.log('No M3U playlist URLs found from any source. Exiting.');
        process.exit(0);
    }

    console.log(`Total unique M3U playlist URLs to process: ${allRemoteM3uPlaylistUrls.length}`);

    const activeChannels = await collectActiveLinks(allRemoteM3uPlaylistUrls, concurrency, fetchTimeout, linkCheckTimeout, batchSize);

    if (activeChannels.length > 0) {
        await saveResults(activeChannels, outputDirPrefix, channelsPerFile, concurrency);
    } else {
        console.log('No active links found after checking all sources.');
        // Ensure the output directory is cleaned up if it exists and is empty
        try {
            const items = await fs.readdir(outputDirPrefix);
            if (items.length === 0) {
                await deleteDirectory(outputDirPrefix);
            }
        } catch (error) {
            if (error.code !== 'ENOENT') {
                 console.warn(`Could not check or delete output directory ${outputDirPrefix}:`, error.message);
            }
        }
    }

    console.log('M3U link collection completed.');
}

main().catch(error => {
    console.error('Script failed with an unhandled error:', error);
    process.exit(1);
});
