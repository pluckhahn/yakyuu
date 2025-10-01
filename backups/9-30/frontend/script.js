// JavaScript for yakyuu.jp homepage

// API base URL
const API_BASE = 'http://localhost:5000/api';

// Search functionality
async function handleSearch() {
    const searchInput = document.querySelector('.search-bar');
    const searchTerm = searchInput.value.trim();
    
    if (searchTerm) {
        try {
            const response = await fetch(`${API_BASE}/search?q=${encodeURIComponent(searchTerm)}`);
            const data = await response.json();
            
            if (data.results && data.results.length > 0) {
                displaySearchResults(data.results);
            } else {
                alert('No results found for: ' + searchTerm);
            }
        } catch (error) {
            console.error('Search error:', error);
            alert('Search failed. Please try again.');
        }
    }
}

// Display search results
function displaySearchResults(results) {
    // Create a modal or dropdown to show results
    let resultsHtml = '<div class="search-results">';
    results.forEach(result => {
        let link = '';
        switch(result.type) {
            case 'player':
                link = `/players/${result.id}`;
                break;
            case 'game':
                link = `/games/${result.id}`;
                break;
            case 'ballpark':
                link = `/ballparks/${result.id}`;
                break;
        }
        
        resultsHtml += `
            <div class="search-result">
                <a href="${link}">
                    <strong>${result.name || result.id}</strong>
                    <span class="result-type">${result.type}</span>
                </a>
            </div>
        `;
    });
    resultsHtml += '</div>';
    
    // For now, just show in console and alert
    console.log('Search results:', results);
    alert(`Found ${results.length} results. Check console for details.`);
}

// Load recent games from API
async function loadRecentGames() {
    const latestGamesDiv = document.getElementById('latest-games');
    
    try {
        const response = await fetch(`${API_BASE}/recent-games`);
        const data = await response.json();
        
        if (data.games && data.games.length > 0) {
            let gamesHtml = '';
            data.games.forEach(game => {
                gamesHtml += `
                    <div class="game-item">
                        <a href="/games/${game.game_id}">
                            <strong>${game.away_team} @ ${game.home_team}</strong><br>
                            <span class="score">${game.score}</span> - ${game.date}<br>
                            <small>${game.ballpark} (${game.gametype})</small>
                        </a>
                    </div>
                `;
            });
            latestGamesDiv.innerHTML = gamesHtml;
        } else {
            latestGamesDiv.innerHTML = '<p>No recent games found.</p>';
        }
    } catch (error) {
        console.error('Error loading recent games:', error);
        latestGamesDiv.innerHTML = '<p>Error loading recent games.</p>';
    }
}

// Load database statistics for hero section
async function loadDatabaseStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        const data = await response.json();
        
        if (data.players !== undefined && data.games !== undefined && data.events !== undefined) {
            // Update the hero stats with real numbers
            const playerStat = document.querySelector('.hero-stat:nth-child(1) .stat-number');
            const gameStat = document.querySelector('.hero-stat:nth-child(2) .stat-number');
            const eventStat = document.querySelector('.hero-stat:nth-child(3) .stat-number');
            
            if (playerStat) playerStat.textContent = data.players.toLocaleString();
            if (gameStat) gameStat.textContent = data.games.toLocaleString();
            if (eventStat) eventStat.textContent = data.events.toLocaleString();
            
            console.log('Database stats loaded:', data);
        }
    } catch (error) {
        console.error('Error loading database stats:', error);
    }
}

// Load league leaders from API
async function loadLeagueLeaders() {
    const leagueLeadersDiv = document.getElementById('league-leaders');
    
    try {
        const response = await fetch(`${API_BASE}/league-leaders`);
        const data = await response.json();
        
        if (data.batting_leaders && data.pitching_leaders) {
            let leadersHtml = '<div class="leaders-section">';
            
            // Batting leaders
            leadersHtml += '<div class="batting-leaders"><h4>Batting Average</h4>';
            data.batting_leaders.forEach(leader => {
                leadersHtml += `
                    <div class="leader-item">
                        <a href="/players/${leader.player_id}">${leader.name}</a>
                        <span class="stat">${leader.stat}</span>
                    </div>
                `;
            });
            leadersHtml += '</div>';
            
            // Pitching leaders
            leadersHtml += '<div class="pitching-leaders"><h4>ERA</h4>';
            data.pitching_leaders.forEach(leader => {
                leadersHtml += `
                    <div class="leader-item">
                        <a href="/players/${leader.player_id}">${leader.name}</a>
                        <span class="stat">${leader.stat}</span>
                    </div>
                `;
            });
            leadersHtml += '</div>';
            
            leadersHtml += '</div>';
            leagueLeadersDiv.innerHTML = leadersHtml;
        } else {
            leagueLeadersDiv.innerHTML = '<p>No league leaders found.</p>';
        }
    } catch (error) {
        console.error('Error loading league leaders:', error);
        leagueLeadersDiv.innerHTML = '<p>Error loading league leaders.</p>';
    }
}

// Add event listeners when the page loads
document.addEventListener('DOMContentLoaded', function() {
    // Add search functionality
    const searchButton = document.querySelector('.search-button');
    const searchBar = document.querySelector('.search-bar');
    
    searchButton.addEventListener('click', handleSearch);
    searchBar.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            handleSearch();
        }
    });
    
    // Load data from API
    loadDatabaseStats();
    loadRecentGames();
    loadLeagueLeaders();
    
    console.log('yakyuu.jp homepage loaded successfully!');
});