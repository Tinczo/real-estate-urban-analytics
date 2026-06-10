\ProvidesClass{labreport}[PWr Lab Report]

\LoadClass{article}

\RequirePackage[english,polish]{babel}
\RequirePackage{graphicx} % Required for inserting images
\RequirePackage{float}
\RequirePackage{polski}
\RequirePackage[skip=10pt plus1pt]{parskip}
\RequirePackage[a4paper, margin=1in, footskip=20pt]{geometry}
\RequirePackage[hidelinks, colorlinks=true, linkcolor=black, urlcolor=blue]{hyperref}
\RequirePackage{minted}
\RequirePackage{relsize}
\RequirePackage{upquote}
\RequirePackage{listings}
\RequirePackage{titlesec}
\RequirePackage{pdflscape}
\RequirePackage{tabularray}
\RequirePackage{tabulary}
\RequirePackage[T1]{fontenc}
\RequirePackage{lmodern}
\RequirePackage{booktabs}
\RequirePackage{multirow}
\RequirePackage{enumitem}
\RequirePackage{titlesec}
\RequirePackage{chngcntr}
\RequirePackage{textgreek}
\RequirePackage{amsmath}
\RequirePackage{subcaption}
\RequirePackage{cleveref}

\crefname{figure}{rysunek}{rysunki}
\crefname{table}{tabela}{tabele}

\counterwithin{figure}{section}
\counterwithin{table}{section}
\counterwithin{equation}{section}

\setlist[itemize]{noitemsep}

\titleclass{\subsubsubsection}{straight}[\subsection]

\newcounter{subsubsubsection}[subsubsection]
\renewcommand\thesubsubsubsection{\thesubsubsection.\arabic{subsubsubsection}}
\renewcommand\theparagraph{\thesubsubsubsection.\arabic{paragraph}} % optional; useful if paragraphs are to be numbered

\titleformat{\subsubsubsection}
  {\normalfont\normalsize\bfseries}{\thesubsubsubsection}{1em}{}
\titlespacing*{\subsubsubsection}
{0pt}{3.25ex plus 1ex minus .2ex}{1.5ex plus .2ex}

\makeatletter
\renewcommand\paragraph{\@startsection{paragraph}{5}{\z@}%
  {3.25ex \@plus1ex \@minus.2ex}%
  {-1em}%
  {\normalfont\normalsize\bfseries}}
\renewcommand\subparagraph{\@startsection{subparagraph}{6}{\parindent}%
  {3.25ex \@plus1ex \@minus .2ex}%
  {-1em}%
  {\normalfont\normalsize\bfseries}}
\def\toclevel@subsubsubsection{4}
\def\toclevel@paragraph{5}
\def\toclevel@paragraph{6}
\def\l@subsubsubsection{\@dottedtocline{4}{7em}{4em}}
\def\l@paragraph{\@dottedtocline{5}{10em}{5em}}
\def\l@subparagraph{\@dottedtocline{6}{14em}{6em}}
\makeatother

\setcounter{secnumdepth}{4}
\setcounter{tocdepth}{4}

\renewcommand{\rmdefault}{lmss}

\newcommand{\reporttitle}[2]{%
    \title{\textbf{#1} \large \\
           #2}
}
