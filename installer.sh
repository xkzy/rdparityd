#!/bin/bash
#
# rtparityd-installer - Install rtparityd storage engine on Linux
#
# Usage: ./installer.sh [--uninstall] [--prefix /usr/local]
#

set -e

VERSION="0.1.0"
PREFIX="${PREFIX:-/usr/local}"
BINDIR="${PREFIX}/bin"
LIBDIR="${PREFIX}/lib/rtparityd"
SYSDDIR="/etc"
SHAREDIR="${PREFIX}/share/rtparityd"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "This script must be run as root"
        exit 1
    fi
}

check_dependencies() {
    local missing=()
    
    if ! command -v go &>/dev/null; then
        missing+=("go")
    fi
    
    if [ ${#missing[@]} -gt 0 ]; then
        log_error "Missing dependencies: ${missing[*]}"
        log_info "Install them with: apt-get install ${missing[*]}"
        exit 1
    fi
}

build() {
    log_info "Building rtparityd..."
    
    mkdir -p dist
    
    go build -ldflags="-s -w -X main.version=${VERSION}" -o dist/rtparityd ./cmd/rtparityd
    go build -ldflags="-s -w -X main.version=${VERSION}" -o dist/rtpctl ./cmd/rtpctl
    
    log_info "Build complete"
}

install_service() {
    log_info "Installing systemd service..."
    mkdir -p "${SYSDDIR}/systemd/system"
    cp packaging/systemd/rtparityd.service "${SYSDDIR}/systemd/system/"
    chmod 644 "${SYSDDIR}/systemd/system/rtparityd.service"
}

install_init() {
    log_info "Installing init script..."
    mkdir -p "${LIBDIR}"
    cp packaging/scripts/rtparityd-init.sh "${LIBDIR}/rtparityd-init"
    chmod 755 "${LIBDIR}/rtparityd-init"
    
    # Add SysV links if not using systemd
    if [ ! -d /run/systemd/system ]; then
        ln -sf "${LIBDIR}/rtparityd-init" /etc/init.d/rtparityd 2>/dev/null || true
    fi
}

install_config() {
    log_info "Installing configuration..."
    mkdir -p "${SYSDDIR}/rtparityd"
    cp packaging/config/config.yaml "${SYSDDIR}/rtparityd/config.yaml"
    chmod 644 "${SYSDDIR}/rtparityd/config.yaml"
}

install_scripts() {
    log_info "Installing CLI tools..."
    mkdir -p "${BINDIR}"
    cp dist/rtparityd "${BINDIR}/"
    cp dist/rtpctl "${BINDIR}/"
    cp packaging/scripts/rtparityctl "${BINDIR}/"
    chmod 755 "${BINDIR}/rtparityd" "${BINDIR}/rtpctl" "${BINDIR}/rtparityctl"
}

install_docs() {
    log_info "Installing documentation and man pages..."
    mkdir -p "${SHAREDIR}/man/man1"
    mkdir -p "${SHAREDIR}/man/man5"
    mkdir -p "${SHAREDIR}/man/man8"
    mkdir -p "${SHAREDIR}/doc"
    
    # Man pages
    cp docs/man/man1/rtpctl.1 "${SHAREDIR}/man/man1/"
    cp docs/man/man1/rtparityctl.1 "${SHAREDIR}/man/man1/"
    cp docs/man/man5/rtparityd.conf.5 "${SHAREDIR}/man/man5/"
    cp docs/man/man8/rtparityd.8 "${SHAREDIR}/man/man8/"
    cp docs/man/man8/rtparityd.service.8 "${SHAREDIR}/man/man8/"
    
    # Documentation
    cp README.md "${SHAREDIR}/doc/"
    cp LICENSE "${SHAREDIR}/doc/"
    
    # Compress man pages
    find "${SHAREDIR}/man" -name "*.1" -o -name "*.5" -o -name "*.8" | xargs -I {} gzip -9 {} 2>/dev/null || true
    
    # Update man database
    if command -v mandb &>/dev/null; then
        mandb -p "${SHAREDIR}/man" 2>/dev/null || true
    fi
}

create_dirs() {
    log_info "Creating runtime directories..."
    mkdir -p /var/lib/rtparityd
    mkdir -p /var/run/rtparityd
    chown root:root /var/lib/rtparityd /var/run/rtparityd
    chmod 755 /var/lib/rtparityd /var/run/rtparityd
}

enable_service() {
    log_info "Enabling service..."
    if command -v systemctl &>/dev/null; then
        systemctl daemon-reload
        systemctl enable rtparityd.service
    fi
}

install() {
    check_root
    check_dependencies
    
    log_info "Installing rtparityd ${VERSION} to ${PREFIX}..."
    
    build
    install_scripts
    install_docs
    install_config
    install_service
    install_init
    create_dirs
    enable_service
    
    log_info ""
    log_info "Installation complete!"
    log_info ""
    log_info "To start the service:"
    log_info "  systemctl start rtparityd"
    log_info ""
    log_info "To check status:"
    log_info "  systemctl status rtparityd"
    log_info "  rtparityctl health"
    log_info ""
    log_info "Socket: /var/run/rtparityd/rtparityd.sock"
    log_info "Data:   /var/lib/rtparityd/"
}

uninstall() {
    check_root
    
    log_info "Uninstalling rtparityd..."
    
    # Stop service
    if command -v systemctl &>/dev/null; then
        systemctl stop rtparityd 2>/dev/null || true
        systemctl disable rtparityd 2>/dev/null || true
    fi
    
    # Remove files
    rm -f "${BINDIR}/rtparityd" "${BINDIR}/rtpctl" "${BINDIR}/rtparityctl"
    rm -rf "${LIBDIR}"
    rm -rf "${SHAREDIR}"
    rm -f "${SYSDDIR}/systemd/system/rtparityd.service"
    rm -f /etc/init.d/rtparityd
    rm -rf "${SYSDDIR}/rtparityd"
    
    # Reload systemd
    if command -v systemctl &>/dev/null; then
        systemctl daemon-reload
    fi
    
    # Optionally remove data
    if [ "$1" = "--purge" ]; then
        log_warn "Removing data directory..."
        rm -rf /var/lib/rtparityd /var/run/rtparityd
    fi
    
    log_info "Uninstall complete"
}

main() {
    case "${1:-}" in
        --uninstall)
            uninstall "${2:-}"
            ;;
        --purge)
            uninstall --purge
            ;;
        --help|-h)
            echo "Usage: $0 [--uninstall|--purge|--help]"
            echo ""
            echo "Options:"
            echo "  --uninstall    Remove rtparityd but keep data"
            echo "  --purge       Remove rtparityd and all data"
            echo "  --help        Show this help"
            ;;
        "")
            install
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
}

main "$@"